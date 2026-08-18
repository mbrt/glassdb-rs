use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb_data::{KeyRef, ObjectPath, TxId};
use glassdb_storage::{
    CurrentState, InlinePolicy, NodeLocks, Requirement, ShardEntry, StorageError,
};

use super::attempt::AttemptState;
use crate::access::{Data, WriteOp};
use crate::error::TransError;
use crate::gc::Gc;
use crate::key_resolver::KeyResolver;
use crate::key_state_resolver::HolderResolution;
use crate::node_locking::NodeLockReconciler;
use crate::shard_coord::{
    CAS_RETRIES, CoordinatedOutcome, FoldOutcome, ReloadCause, ResolveCtx, ShardCoordinator,
    ShardResolver, StageAdmission, Step,
};
use crate::split::SplitHintSink;

/// Direct same-leaf commit coverage for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectCommitStats {
    /// Mutation attempts shaped and routed for the direct path.
    pub candidates: u64,
    /// Candidates that committed directly.
    pub landed: u64,
}

impl AddAssign for DirectCommitStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.landed += rhs.landed;
    }
}

impl Sub for DirectCommitStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            landed: self.landed.saturating_sub(rhs.landed),
        }
    }
}

#[derive(Default)]
struct DirectCommitCounters {
    candidates: AtomicU64,
    landed: AtomicU64,
}

/// Owns the logless same-leaf commit subprotocol.
#[derive(Clone)]
pub(super) struct DirectCommit {
    resolver: KeyResolver,
    coord: ShardCoordinator,
    inline_policy: InlinePolicy,
    split_hints: SplitHintSink,
    gc: Gc,
    counters: Arc<DirectCommitCounters>,
}

impl DirectCommit {
    /// Creates the direct-commit path over the engine's shared collaborators.
    pub(super) fn new(
        resolver: KeyResolver,
        coord: ShardCoordinator,
        inline_policy: InlinePolicy,
        split_hints: SplitHintSink,
        gc: Gc,
    ) -> Self {
        DirectCommit {
            resolver,
            coord,
            inline_policy,
            split_hints,
            gc,
            counters: Arc::new(DirectCommitCounters::default()),
        }
    }

    /// Returns and resets direct-commit coverage counters.
    pub(super) fn stats_and_reset(&self) -> DirectCommitStats {
        DirectCommitStats {
            candidates: self.counters.candidates.swap(0, Ordering::Relaxed),
            landed: self.counters.landed.swap(0, Ordering::Relaxed),
        }
    }

    /// Attempts one atomic logless commit for a complete point transaction.
    ///
    /// An eligible member publishes every output in one conditional leaf CAS.
    /// It creates no transaction object or lock and has no write-back phase.
    /// Certified losses either replay the body or use the regular locked path;
    /// an unresolved CAS is never rerun as a new transaction (ADR-061).
    pub(super) async fn try_commit(
        &self,
        id: &TxId,
        data: &Data,
        state: &mut AttemptState,
    ) -> Result<DirectAttempt, TransError> {
        let Some(member) = direct_shape(data) else {
            return Ok(DirectAttempt::Locked);
        };
        let Some(mut leaf_path) = self.route_member(&member).await? else {
            return Ok(DirectAttempt::Locked);
        };
        self.counters.candidates.fetch_add(1, Ordering::Relaxed);

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
            return Ok(DirectAttempt::Locked);
        }

        for reroute in 0..CAS_RETRIES {
            let resolver = Arc::new(DirectCommitResolver::new(
                id.clone(),
                leaf_path.clone(),
                member.clone(),
                self.inline_policy,
                self.split_hints.clone(),
            ));
            let outcome = self
                .coord
                .submit_shard(&leaf_path, id, resolver.clone(), Requirement::Any)
                .await?;
            match outcome {
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Landed,
                    ..
                }) => {
                    self.counters.landed.fetch_add(1, Ordering::Relaxed);
                    state.commit();
                    for predecessor in resolver.predecessors() {
                        self.gc.schedule_tx_cleanup(predecessor);
                    }
                    return Ok(DirectAttempt::Committed);
                }
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::InDoubt(msg),
                    ..
                }) => return Err(TransError::Storage(StorageError::Unavailable(msg))),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Replay,
                    ..
                }) => return Ok(DirectAttempt::Replay),
                Some(CoordinatedOutcome {
                    outcome: FoldOutcome::Reroute,
                    ..
                }) if reroute + 1 < CAS_RETRIES => {
                    let Some(path) = self.route_member(&member).await? else {
                        return Ok(DirectAttempt::Locked);
                    };
                    leaf_path = path;
                }
                Some(CoordinatedOutcome {
                    outcome:
                        FoldOutcome::Moved
                        | FoldOutcome::Conflict
                        | FoldOutcome::LeafFull
                        | FoldOutcome::Reroute,
                    ..
                })
                | None => return Ok(DirectAttempt::Locked),
                Some(_) => {
                    return Err(TransError::other(
                        "direct commit produced a non-commit outcome",
                    ));
                }
            }
        }
        Ok(DirectAttempt::Locked)
    }

    /// Returns the one leaf currently owning every dependency in `member`.
    async fn route_member(&self, member: &DirectMember) -> Result<Option<ObjectPath>, TransError> {
        let keys: Vec<KeyRef> = member.keys.iter().map(|key| key.key.clone()).collect();
        self.resolver.route_one_leaf(keys).await.map_err(Into::into)
    }
}

/// One normalized point dependency and its optional final mutation.
#[derive(Clone)]
struct DirectKey {
    key: KeyRef,
    raw_key: Vec<u8>,
    read: Option<ReadPredicate>,
    write: Option<DirectWrite>,
}

/// The exact writer observed by a point read. The outer option on
/// [`DirectKey::read`] distinguishes a blind write from a read of unmarked
/// absence, whose expected writer is `None`.
#[derive(Clone)]
struct ReadPredicate {
    writer: Option<TxId>,
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
struct DirectCommitResolver {
    id: TxId,
    leaf_path: ObjectPath,
    member: DirectMember,
    inline: InlinePolicy,
    split_hints: SplitHintSink,
    /// Output writers replaced by the last proposed publication. The outer
    /// option distinguishes "not staged" from a staged create over absence.
    staged_over: Mutex<Option<BTreeMap<Vec<u8>, Option<TxId>>>>,
    /// Once any exact output marker is observed, the leaf CAS atomically proves
    /// the whole member landed even if a later fold or CAS replaces it.
    landed_proven: AtomicBool,
}

impl DirectCommitResolver {
    fn new(
        id: TxId,
        leaf_path: ObjectPath,
        member: DirectMember,
        inline: InlinePolicy,
        split_hints: SplitHintSink,
    ) -> Self {
        Self {
            id,
            leaf_path,
            member,
            inline,
            split_hints,
            staged_over: Mutex::new(None),
            landed_proven: AtomicBool::new(false),
        }
    }

    /// Returns distinct committed writers displaced by the landed member.
    fn predecessors(&self) -> Vec<TxId> {
        let mut predecessors = BTreeSet::new();
        if let Some(staged) = self.staged_over.lock().unwrap().as_ref() {
            predecessors.extend(staged.values().flatten().cloned());
        }
        predecessors.into_iter().collect()
    }

    /// Resolves all dependencies against one running fold state.
    async fn resolve_keys(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
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

    /// Produces the ordinary policy decision after uncertainty, if any, has
    /// been resolved as a definite non-landing.
    async fn resolve_fresh(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
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
        if NodeLockReconciler::new(ctx.key_state, ctx.tmon, &self.id)
            .admit_direct(&mut locks, changes_membership)
            .await?
            .is_some()
        {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Moved,
            });
        }

        // Coordination blockers win over a stale-read replay. Retrying a body
        // while the same live holder remains would otherwise spin.
        if resolutions.iter().any(|state| !state.pending.is_empty()) {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Moved,
            });
        }
        if self
            .member
            .keys
            .iter()
            .zip(resolutions)
            .any(|(key, state)| {
                key.read
                    .as_ref()
                    .is_some_and(|read| read.writer != state.writer)
            })
        {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Replay,
            });
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
                outcome: FoldOutcome::Moved,
            });
        }

        let mut entries = Vec::with_capacity(self.member.writes);
        let mut predecessors = BTreeMap::new();
        let mut adds_key = false;
        for (key, state) in self.member.keys.iter().zip(resolutions) {
            let Some(write) = &key.write else {
                continue;
            };
            predecessors.insert(key.raw_key.clone(), state.writer.clone());
            let current = match write {
                DirectWrite::Put(value) => {
                    adds_key |= state.writer.is_none() || state.deleted;
                    CurrentState::Inline {
                        writer: self.id.clone(),
                        value: value.clone(),
                    }
                }
                DirectWrite::Delete => CurrentState::Tombstone {
                    writer: self.id.clone(),
                },
            };
            entries.push((
                key.raw_key.clone(),
                ShardEntry::new(key.raw_key.clone()).with_current(current),
            ));
        }
        if changes_membership {
            locks.advance_membership_version();
        }
        *self.staged_over.lock().unwrap() = Some(predecessors);
        Ok(Step::Stage {
            entries,
            locks,
            admission: StageAdmission::InlinePublication {
                adds_key,
                pressure_hint: self.member.keys.len() == 1,
            },
            outcome: FoldOutcome::Landed,
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
        self.split_hints
            .observe_inline_pressure(&self.leaf_path, &key.raw_key, value_len);
    }

    /// Whether any current state is an exact output marker for this member.
    fn has_marker(&self, entries: &BTreeMap<Vec<u8>, ShardEntry>) -> bool {
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
        self.member
            .keys
            .iter()
            .zip(resolutions)
            .filter(|(key, _)| key.write.is_some())
            .all(|(key, state)| staged.get(&key.raw_key) == Some(&state.writer))
    }

    fn proven_landed(&self) -> bool {
        self.landed_proven.load(Ordering::Acquire)
    }

    fn remember_landed(&self) {
        self.landed_proven.store(true, Ordering::Release);
    }

    fn known_or(&self, in_doubt: bool, otherwise: FoldOutcome) -> FoldOutcome {
        if self.proven_landed() {
            FoldOutcome::Landed
        } else if in_doubt {
            self.ambiguous_outcome()
        } else {
            otherwise
        }
    }

    fn ambiguous_outcome(&self) -> FoldOutcome {
        FoldOutcome::InDoubt(format!(
            "direct commit for {} could not be resolved after an uncertain CAS",
            self.id
        ))
    }

    fn definitive_loss(&self) -> FoldOutcome {
        if self.member.has_reads {
            FoldOutcome::Replay
        } else {
            FoldOutcome::Moved
        }
    }
}

#[async_trait]
impl ShardResolver for DirectCommitResolver {
    fn observe_loaded(&self, entries: &BTreeMap<Vec<u8>, ShardEntry>) {
        if self.has_marker(entries) {
            self.remember_landed();
        }
    }

    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        if self.proven_landed() || self.has_marker(staged) {
            self.remember_landed();
            return Ok(Step::Claim {
                outcome: FoldOutcome::Landed,
            });
        }

        let resolutions = self.resolve_keys(ctx, staged).await?;
        let in_doubt = matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true });
        if in_doubt && !self.proves_non_landing(&resolutions) {
            return Ok(Step::Skip {
                outcome: self.ambiguous_outcome(),
            });
        }
        let step = self
            .resolve_fresh(ctx, staged, staged_locks, &resolutions)
            .await?;
        if in_doubt {
            Ok(Step::Recovered {
                step: Box::new(step),
            })
        } else {
            Ok(step)
        }
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
        self.known_or(in_doubt, FoldOutcome::Moved)
    }

    fn reroute_outcome(&self, in_doubt: bool) -> FoldOutcome {
        self.known_or(in_doubt, FoldOutcome::Reroute)
    }

    fn excluded_outcome(&self, in_doubt: bool) -> FoldOutcome {
        self.known_or(in_doubt, self.definitive_loss())
    }

    fn owned_keys(&self) -> Vec<&[u8]> {
        self.member
            .keys
            .iter()
            .map(|key| key.raw_key.as_slice())
            .collect()
    }

    fn logless_keys(&self) -> Vec<&[u8]> {
        self.member
            .output_keys()
            .map(|key| key.raw_key.as_slice())
            .collect()
    }
}

/// What an attempted direct commit established about its transaction.
pub(super) enum DirectAttempt {
    /// The one-CAS commit landed.
    Committed,
    /// Nothing durable landed and read-dependent computation must be rerun.
    Replay,
    /// The regular logged protocol must coordinate the transaction.
    Locked,
}

/// Recognizes and normalizes a complete point mutation transaction.
fn direct_shape(data: &Data) -> Option<DirectMember> {
    if data.writes.is_empty() || !data.scans.is_empty() {
        return None;
    }
    let mut keys: BTreeMap<KeyRef, DirectKey> = BTreeMap::new();
    for read in &data.reads {
        let key = keys.entry(read.key.clone()).or_insert_with(|| DirectKey {
            key: read.key.clone(),
            raw_key: read.key.key().to_vec(),
            read: None,
            write: None,
        });
        key.read = Some(ReadPredicate {
            writer: read.last_writer().cloned(),
        });
    }
    for write in &data.writes {
        let key = keys.entry(write.key.clone()).or_insert_with(|| DirectKey {
            key: write.key.clone(),
            raw_key: write.key.key().to_vec(),
            read: None,
            write: None,
        });
        key.write = Some(match &write.op {
            WriteOp::Put(value) => DirectWrite::Put(value.clone()),
            WriteOp::Delete => DirectWrite::Delete,
        });
    }
    Some(DirectMember {
        keys: keys.into_values().collect(),
        writes: data.writes.len(),
        has_reads: !data.reads.is_empty(),
    })
}

#[cfg(test)]
mod tests;
