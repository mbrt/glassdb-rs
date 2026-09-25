use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb_data::{LogicalKey, ObjectPath, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{
    CurrentState, InlinePolicy, LeafEntry, Node, NodeLocks, Requirement, RoutedLeafGroup,
    StorageError, TreeRouter,
};

use super::handle_state::HandleState;
use crate::access::{AccessSet, ReadPredicate, WriteOp};
use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_state_resolver::HolderResolution;
use crate::leaf_coord::{
    CoordinatedOutcome, LeafCoordinator, LeafOperation, MemberOutcome, MemberPolicy, ReloadCause,
    ResolveCtx, StageAdmission, Step,
};
use crate::structural::StructuralHintSink;

/// Direct same-leaf commit coverage for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectCommitStats {
    /// Mutation attempts shaped and routed for direct commit.
    pub candidates: u64,
    /// Candidates that committed directly.
    pub landed: u64,
    /// Fallbacks to a locked commit because the point accesses of an otherwise
    /// eligible attempt route to two leaves, where the right link of one leaf
    /// names the other. A reroute after a concurrent split can also cause one.
    pub cross_leaf_adjacent: u64,
    /// Fallbacks to a locked commit because the point accesses of an otherwise
    /// eligible attempt route to more than two leaves, or to two leaves that
    /// are not adjacent.
    pub cross_leaf_scattered: u64,
}

impl AddAssign for DirectCommitStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.landed += rhs.landed;
        self.cross_leaf_adjacent += rhs.cross_leaf_adjacent;
        self.cross_leaf_scattered += rhs.cross_leaf_scattered;
    }
}

impl Sub for DirectCommitStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            landed: self.landed.saturating_sub(rhs.landed),
            cross_leaf_adjacent: self
                .cross_leaf_adjacent
                .saturating_sub(rhs.cross_leaf_adjacent),
            cross_leaf_scattered: self
                .cross_leaf_scattered
                .saturating_sub(rhs.cross_leaf_scattered),
        }
    }
}

#[derive(Default)]
struct DirectCommitCounters {
    candidates: AtomicU64,
    landed: AtomicU64,
    cross_leaf_adjacent: AtomicU64,
    cross_leaf_scattered: AtomicU64,
}

/// The leaves to which the point accesses of one direct member route.
enum MemberPlacement {
    OneLeaf(ObjectPath),
    AdjacentLeaves,
    ScatteredLeaves,
}

/// Owns the direct same-leaf commit subprotocol.
#[derive(Clone)]
pub(super) struct DirectCommit {
    router: TreeRouter,
    coord: LeafCoordinator,
    inline_policy: InlinePolicy,
    structural_hints: StructuralHintSink,
    gc_hints: GcHints,
    counters: Arc<DirectCommitCounters>,
}

impl DirectCommit {
    /// Creates direct commit over the engine's shared collaborators.
    pub(super) fn new(
        router: TreeRouter,
        coord: LeafCoordinator,
        inline_policy: InlinePolicy,
        structural_hints: StructuralHintSink,
        gc_hints: GcHints,
    ) -> Self {
        DirectCommit {
            router,
            coord,
            inline_policy,
            structural_hints,
            gc_hints,
            counters: Arc::new(DirectCommitCounters::default()),
        }
    }

    /// Returns and resets direct-commit coverage counters.
    pub(super) fn stats_and_reset(&self) -> DirectCommitStats {
        DirectCommitStats {
            candidates: self.counters.candidates.swap(0, Ordering::Relaxed),
            landed: self.counters.landed.swap(0, Ordering::Relaxed),
            cross_leaf_adjacent: self.counters.cross_leaf_adjacent.swap(0, Ordering::Relaxed),
            cross_leaf_scattered: self
                .counters
                .cross_leaf_scattered
                .swap(0, Ordering::Relaxed),
        }
    }

    /// Attempts one atomic direct commit for a complete point transaction.
    ///
    /// An eligible member publishes every output in one conditional leaf CAS.
    /// It creates no transaction record or lock and has no write-back phase.
    /// Certified losses either replay the body or fall back to a locked commit;
    /// an unresolved in-doubt CAS never leads to a body replay (ADR-061).
    pub(super) async fn try_commit(
        &self,
        id: &TxId,
        accesses: &AccessSet,
        state: &mut HandleState,
    ) -> Result<DirectOutcome, TransError> {
        let Some(member) = direct_member(accesses) else {
            return Ok(DirectOutcome::Locked);
        };
        // A zero policy disables the protocol even for all-delete members. All
        // put bytes must be durable in the commit leaf itself.
        if !self.inline_policy.admits_value(0)
            || member.keys.iter().any(|key| {
                matches!(
                    &key.write,
                    Some(DirectWrite::Put(value))
                        if !self.inline_policy.admits_value(value.len())
                )
            })
        {
            return Ok(DirectOutcome::Locked);
        }
        let Some(mut leaf_path) = self.single_leaf(self.route_member(&member).await?) else {
            return Ok(DirectOutcome::Locked);
        };
        self.counters.candidates.fetch_add(1, Ordering::Relaxed);

        // A split can stale the path between grouping and submission. One fresh
        // regroup preserves direct commit for that race; repeated topology
        // churn falls back instead of borrowing the coordinator's CAS budget.
        let mut rerouted = false;
        loop {
            let operation = DirectCommitOperation::new(
                *id,
                leaf_path.clone(),
                member.clone(),
                self.inline_policy,
                self.structural_hints.clone(),
            );
            let outcome = self.coord.coordinate(operation).await?;
            match outcome {
                DirectMutationOutcome::Landed(predecessors) => {
                    self.counters.landed.fetch_add(1, Ordering::Relaxed);
                    state.commit();
                    self.gc_hints.schedule_all(predecessors);
                    return Ok(DirectOutcome::Committed);
                }
                DirectMutationOutcome::InDoubt(msg) => {
                    return Err(TransError::Storage(StorageError::Unavailable(msg)));
                }
                DirectMutationOutcome::Replay => return Ok(DirectOutcome::Replay),
                DirectMutationOutcome::Reroute if !rerouted => {
                    let Some(path) = self.single_leaf(self.route_member(&member).await?) else {
                        return Ok(DirectOutcome::Locked);
                    };
                    leaf_path = path;
                    rerouted = true;
                }
                DirectMutationOutcome::Locked | DirectMutationOutcome::Reroute => {
                    return Ok(DirectOutcome::Locked);
                }
            }
        }
    }

    /// Finds the leaves to which the dependencies of `member` route.
    async fn route_member(&self, member: &DirectMember) -> Result<MemberPlacement, TransError> {
        let keys = member
            .keys
            .iter()
            .map(|key| (key.key.clone(), ()))
            .collect::<Vec<_>>();
        let groups = self
            .router
            .route_keys_with_requirements(keys, Requirement::ANY, Requirement::ANY)
            .await?;
        Ok(match groups.as_slice() {
            [group] => MemberPlacement::OneLeaf(group.path().clone()),
            [a, b] if links_right_to(a, b) || links_right_to(b, a) => {
                MemberPlacement::AdjacentLeaves
            }
            _ => MemberPlacement::ScatteredLeaves,
        })
    }

    /// Returns the leaf of a one-leaf `placement`, and counts any other
    /// placement as a cross-leaf fallback.
    fn single_leaf(&self, placement: MemberPlacement) -> Option<ObjectPath> {
        let counter = match placement {
            MemberPlacement::OneLeaf(path) => return Some(path),
            MemberPlacement::AdjacentLeaves => &self.counters.cross_leaf_adjacent,
            MemberPlacement::ScatteredLeaves => &self.counters.cross_leaf_scattered,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        None
    }
}

/// Reports whether the right link of the leaf of `left` names the leaf of
/// `right`.
fn links_right_to(left: &RoutedLeafGroup<()>, right: &RoutedLeafGroup<()>) -> bool {
    let (
        ObjectPath::Node {
            collection: left_collection,
            ..
        },
        ObjectPath::Node { collection, id },
    ) = (left.path(), right.path())
    else {
        return false;
    };
    left_collection == collection && left.node().and_then(Node::right_sibling) == Some(*id)
}

/// One normalized point dependency and its optional final mutation.
#[derive(Clone)]
struct DirectKey {
    key: LogicalKey,
    raw_key: Vec<u8>,
    read: Option<ReadPredicate>,
    write: Option<DirectWrite>,
}

/// A directly publishable final mutation.
#[derive(Clone)]
enum DirectWrite {
    Put(Arc<[u8]>),
    Delete,
}

/// A complete point-access transaction in deterministic key order.
#[derive(Clone)]
struct DirectMember {
    keys: Arc<[DirectKey]>,
    writes: usize,
    has_reads: bool,
}

impl DirectMember {
    fn output_keys(&self) -> impl Iterator<Item = &DirectKey> {
        self.keys.iter().filter(|key| key.write.is_some())
    }
}

/// Commits one complete same-leaf point transaction in a single leaf CAS.
struct DirectCommitOperation {
    id: TxId,
    leaf_path: ObjectPath,
    member: DirectMember,
    inline: InlinePolicy,
    structural_hints: StructuralHintSink,
    /// Output states replaced by the last proposed publication. The outer
    /// option distinguishes "not staged" from a staged create over absence.
    staged_over: Mutex<Option<BTreeMap<Vec<u8>, CurrentState>>>,
    /// Once any exact output marker is observed, the leaf CAS atomically proves
    /// the whole member landed even if a later planned change or CAS replaces it.
    landed_proven: AtomicBool,
}

impl DirectCommitOperation {
    fn new(
        id: TxId,
        leaf_path: ObjectPath,
        member: DirectMember,
        inline: InlinePolicy,
        structural_hints: StructuralHintSink,
    ) -> Self {
        Self {
            id,
            leaf_path,
            member,
            inline,
            structural_hints,
            staged_over: Mutex::new(None),
            landed_proven: AtomicBool::new(false),
        }
    }

    /// Returns transaction-record references displaced by the landed member.
    fn predecessors(&self) -> Vec<TxId> {
        let mut predecessors = BTreeSet::new();
        if let Some(staged) = self.staged_over.lock().unwrap().as_ref() {
            // Inline values and tombstones can have direct-commit writers. Scans find
            // any transaction records that remain behind those states.
            predecessors.extend(staged.values().filter_map(|state| match state {
                CurrentState::External { writer } => Some(*writer),
                _ => None,
            }));
        }
        predecessors.into_iter().collect()
    }

    /// Resolves all dependencies against one staged leaf state.
    async fn resolve_keys(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
    ) -> Result<Vec<HolderResolution>, TransError> {
        let mut resolutions = Vec::with_capacity(self.member.keys.len());
        for key in self.member.keys.iter() {
            resolutions.push(
                ctx.key_state
                    .resolve_holders(&key.key, staged.get(&key.raw_key), None, ctx.requirement)
                    .await?,
            );
        }
        Ok(resolutions)
    }

    /// Validates node-level coordination for a direct publication.
    async fn reconcile_node_blockers(
        &self,
        ctx: &ResolveCtx<'_>,
        locks: &mut NodeLocks,
        changes_membership: bool,
    ) -> Result<bool, TransError> {
        // Pruning only the staged copy keeps this outside the lock lifecycle:
        // removals of holders with a final status become durable iff the
        // publication CAS lands.
        if let Some(holder) = locks.drop_intent().cloned() {
            match ctx.tmon.tx_status(&holder).await? {
                TxCommitStatus::Committed => return Err(TransError::StaleCollection),
                TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                    locks.remove_drop_intent(&holder);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => return Ok(true),
            }
        }

        for holder in locks.structural_gate().holders().to_vec() {
            match ctx.tmon.tx_status(&holder).await? {
                TxCommitStatus::Committed | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                    locks.remove_structural_gate(&holder);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => return Ok(true),
            }
        }

        if changes_membership {
            for holder in locks.membership().holders().to_vec() {
                match ctx.tmon.tx_status(&holder).await? {
                    TxCommitStatus::Committed
                    | TxCommitStatus::Aborted
                    | TxCommitStatus::Wounded => {
                        locks.remove_membership_holder(&holder);
                    }
                    TxCommitStatus::Pending | TxCommitStatus::Unknown => return Ok(true),
                }
            }
        }
        Ok(false)
    }

    /// Produces the ordinary policy decision after an in-doubt CAS, if any, has
    /// been resolved as a definite non-landing.
    async fn resolve_fresh(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
        resolutions: &[HolderResolution],
    ) -> Result<Step, TransError> {
        let changes_membership = self
            .member
            .keys
            .iter()
            .zip(resolutions)
            .any(|(key, state)| match key.write {
                Some(DirectWrite::Put(_)) => state.writer.is_none() || state.deleted,
                Some(DirectWrite::Delete) => state.writer.is_some() && !state.deleted,
                None => false,
            });

        let mut locks = staged_locks.clone();
        if self
            .reconcile_node_blockers(ctx, &mut locks, changes_membership)
            .await?
        {
            return Ok(Step::Skip {
                outcome: MemberOutcome::Moved,
            });
        }

        // Coordination blockers win over an invalidated-read replay. Replaying a body
        // while the same live holder remains would otherwise spin.
        if resolutions.iter().any(|state| !state.pending.is_empty()) {
            return Ok(Step::Skip {
                outcome: MemberOutcome::Moved,
            });
        }
        if self
            .member
            .keys
            .iter()
            .zip(resolutions)
            .any(|(key, state)| {
                key.read.as_ref().is_some_and(|read| {
                    !read.validates(state.writer.as_ref(), locks.membership_generation())
                })
            })
        {
            // Releasing a membership writer with a final status advances the staged
            // generation. A skipped publication would discard that change, so
            // replaying the same absence read could never converge. The locked
            // path makes the cleanup durable before validating the read.
            let outcome = if locks.membership_generation() != staged_locks.membership_generation() {
                MemberOutcome::Moved
            } else {
                MemberOutcome::Replay
            };
            return Ok(Step::Skip { outcome });
        }

        let output_keys: BTreeSet<&[u8]> = self
            .member
            .output_keys()
            .map(|key| key.raw_key.as_slice())
            .collect();
        let retained_inline = staged
            .iter()
            .filter(|(key, _)| !output_keys.contains(key.as_slice()))
            .try_fold(0usize, |total, (_, entry)| {
                total.checked_add(entry.current.inline_len())
            });
        let output_inline = self.member.output_keys().try_fold(0usize, |total, key| {
            let len = match key.write.as_ref().expect("output key has a write") {
                DirectWrite::Put(value) => value.len(),
                DirectWrite::Delete => 0,
            };
            total.checked_add(len)
        });
        let admitted = retained_inline
            .zip(output_inline)
            .and_then(|(retained, output)| retained.checked_add(output))
            .is_some_and(|total| total <= self.inline.max_leaf_bytes);
        if !admitted {
            self.observe_pressure();
            return Ok(Step::Skip {
                outcome: MemberOutcome::Moved,
            });
        }

        let mut entries = Vec::with_capacity(self.member.writes);
        let mut predecessors = BTreeMap::new();
        let mut adds_key = false;
        for (key, state) in self.member.keys.iter().zip(resolutions) {
            let Some(write) = &key.write else {
                continue;
            };
            predecessors.insert(
                key.raw_key.clone(),
                state.resolved_current(staged.get(&key.raw_key)),
            );
            let current = match write {
                DirectWrite::Put(value) => {
                    adds_key |= state.writer.is_none() || state.deleted;
                    CurrentState::Inline {
                        writer: self.id,
                        value: value.clone(),
                    }
                }
                DirectWrite::Delete => CurrentState::Tombstone { writer: self.id },
            };
            entries.push((
                key.raw_key.clone(),
                LeafEntry::new(key.raw_key.clone()).with_current(current),
            ));
        }
        if changes_membership {
            locks.advance_membership_generation();
        }
        *self.staged_over.lock().unwrap() = Some(predecessors);
        Ok(Step::Stage {
            entries,
            locks,
            admission: StageAdmission::InlinePublication {
                adds_key,
                pressure_hint: self.member.keys.len() == 1,
            },
            outcome: MemberOutcome::Landed,
        })
    }

    /// Reports single-key aggregate pressure without steering multi-key
    /// transactions toward a split that could separate their dependencies.
    fn observe_pressure(&self) {
        if self.member.keys.len() != 1 {
            return;
        }
        let key = self
            .member
            .output_keys()
            .next()
            .expect("a direct member has at least one output");
        let value_len = match key.write.as_ref().expect("output key has a write") {
            DirectWrite::Put(value) => value.len(),
            DirectWrite::Delete => 0,
        };
        if !self.inline.admits_value(value_len) {
            return;
        }
        self.structural_hints
            .observe_inline_pressure(&self.leaf_path, &key.raw_key, value_len);
    }

    /// Whether any current state is an exact output marker for this member.
    fn has_marker(&self, entries: &BTreeMap<Vec<u8>, LeafEntry>) -> bool {
        self.member.output_keys().any(|key| {
            entries
                .get(&key.raw_key)
                .is_some_and(|entry| self.is_marker(&entry.current, key))
        })
    }

    fn is_marker(&self, current: &CurrentState, key: &DirectKey) -> bool {
        if current.writer() != Some(&self.id) {
            return false;
        }
        match key.write.as_ref().expect("marker checks an output key") {
            DirectWrite::Put(value) => current.inline() == Some(value),
            DirectWrite::Delete => current.is_tombstone(),
        }
    }

    /// Proves that the last attempted CAS left every output untouched.
    fn proves_non_landing(&self, resolutions: &[HolderResolution]) -> bool {
        let staged = self.staged_over.lock().unwrap();
        let Some(staged) = staged.as_ref() else {
            return false;
        };
        let mut durable_witness = false;
        for (key, state) in self
            .member
            .keys
            .iter()
            .zip(resolutions)
            .filter(|(key, _)| key.write.is_some())
        {
            let predecessor = staged
                .get(&key.raw_key)
                .expect("every direct output records its predecessor");
            if predecessor.writer() != state.writer.as_ref() {
                return false;
            }
            durable_witness |= match key.write.as_ref().expect("output key has a write") {
                DirectWrite::Put(_) => true,
                DirectWrite::Delete => predecessor.writer().is_some(),
            };
        }
        durable_witness
    }

    fn proven_landed(&self) -> bool {
        self.landed_proven.load(Ordering::Acquire)
    }

    fn remember_landed(&self) {
        self.landed_proven.store(true, Ordering::Release);
    }

    fn known_or(&self, in_doubt: bool, otherwise: MemberOutcome) -> MemberOutcome {
        if self.proven_landed() {
            MemberOutcome::Landed
        } else if in_doubt {
            self.in_doubt_outcome()
        } else {
            otherwise
        }
    }

    fn in_doubt_outcome(&self) -> MemberOutcome {
        MemberOutcome::InDoubt(format!(
            "direct commit for {} could not be resolved after an in-doubt CAS",
            self.id
        ))
    }

    fn definitive_loss(&self) -> MemberOutcome {
        if self.member.has_reads {
            MemberOutcome::Replay
        } else {
            MemberOutcome::Moved
        }
    }
}

#[async_trait]
impl MemberPolicy for DirectCommitOperation {
    fn observe_loaded(&self, entries: &BTreeMap<Vec<u8>, LeafEntry>) {
        if self.has_marker(entries) {
            self.remember_landed();
        }
    }

    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        if self.proven_landed() || self.has_marker(staged) {
            self.remember_landed();
            return Ok(Step::Skip {
                outcome: MemberOutcome::Landed,
            });
        }

        let resolutions = self.resolve_keys(ctx, staged).await?;
        let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
        if in_doubt && !self.proves_non_landing(&resolutions) {
            return Ok(Step::Skip {
                outcome: self.in_doubt_outcome(),
            });
        }
        self.resolve_fresh(ctx, staged, staged_locks, &resolutions)
            .await
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, in_doubt: bool) -> MemberOutcome {
        self.known_or(in_doubt, MemberOutcome::Moved)
    }

    fn reroute_outcome(&self, in_doubt: bool) -> MemberOutcome {
        self.known_or(in_doubt, MemberOutcome::Reroute)
    }

    fn excluded_outcome(&self, in_doubt: bool) -> MemberOutcome {
        self.known_or(in_doubt, self.definitive_loss())
    }

    fn leaf_scope_keys(&self) -> Vec<&[u8]> {
        self.member
            .keys
            .iter()
            .map(|key| key.raw_key.as_slice())
            .collect()
    }

    fn direct_publication_keys(&self) -> Vec<&[u8]> {
        self.member
            .output_keys()
            .map(|key| key.raw_key.as_slice())
            .collect()
    }
}

impl LeafOperation for DirectCommitOperation {
    type Output = DirectMutationOutcome;

    fn path(&self) -> &ObjectPath {
        &self.leaf_path
    }

    fn id(&self) -> &TxId {
        &self.id
    }

    fn requirement(&self) -> Requirement {
        // New publication requires CAS against the observed revision. An exact
        // output marker for this non-reused identity instead proves an earlier
        // publication; it does not need to prove that the output is still current.
        Requirement::ANY
    }

    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError> {
        match outcome {
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Landed,
                ..
            }) => Ok(DirectMutationOutcome::Landed(self.predecessors())),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::InDoubt(message),
                ..
            }) => Ok(DirectMutationOutcome::InDoubt(message)),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Replay,
                ..
            }) => Ok(DirectMutationOutcome::Replay),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Reroute,
                ..
            }) => Ok(DirectMutationOutcome::Reroute),
            Some(CoordinatedOutcome {
                outcome: MemberOutcome::Moved | MemberOutcome::Conflict | MemberOutcome::LeafFull,
                ..
            })
            | None => Ok(DirectMutationOutcome::Locked),
            Some(_) => Err(TransError::other(
                "direct commit produced a non-commit outcome",
            )),
        }
    }
}

/// Result of one direct-commit operation at the leaf-mutation seam.
enum DirectMutationOutcome {
    Landed(Vec<TxId>),
    InDoubt(String),
    Replay,
    Reroute,
    Locked,
}

/// What a tried direct commit established about its transaction.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DirectOutcome {
    /// The one-CAS commit landed.
    Committed,
    /// Nothing durable landed and the body must be replayed.
    Replay,
    /// A locked commit must coordinate the transaction.
    Locked,
}

/// Converts the access set's complete point-mutation shape into direct-commit
/// state.
fn direct_member(accesses: &AccessSet) -> Option<DirectMember> {
    let shape = accesses.direct_shape()?;
    let keys = shape
        .points()
        .map(|point| DirectKey {
            key: point.key.clone(),
            raw_key: point.key.key().to_vec(),
            read: point.read.map(|read| read.predicate().clone()),
            write: point.write.map(|write| match write.operation() {
                WriteOp::Put(value) => DirectWrite::Put(value.clone()),
                WriteOp::Delete => DirectWrite::Delete,
            }),
        })
        .collect::<Vec<_>>();
    Some(DirectMember {
        keys: keys.into(),
        writes: shape.write_count(),
        has_reads: shape.read_count() != 0,
    })
}

#[cfg(test)]
mod tests;
