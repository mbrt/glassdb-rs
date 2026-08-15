use std::ops::{AddAssign, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use glassdb_data::{KeyRef, ObjectPath, TxId};
use glassdb_storage::{
    CurrentState, InlinePolicy, LockType, NodeLocks, Requirement, ShardEntry, StorageError,
};

use super::attempt::AttemptState;
use crate::access::{Data, WriteOp};
use crate::error::TransError;
use crate::gc::Gc;
use crate::key_resolver::KeyResolver;
use crate::key_state_resolver::HolderResolution;
use crate::shard_coord::{
    CoordinatedOutcome, FoldOutcome, ReloadCause, ResolveCtx, ShardCoordinator, ShardResolver,
    StageAdmission, Step,
};
use crate::split::SplitHintSink;

/// Direct single-key commit coverage for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectCommitStats {
    /// Mutation attempts shaped for the direct path.
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

/// Owns the logless single-key commit subprotocol.
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

    /// Returns and resets direct single-key commit coverage counters.
    pub(super) fn stats_and_reset(&self) -> DirectCommitStats {
        DirectCommitStats {
            candidates: self.counters.candidates.swap(0, Ordering::Relaxed),
            landed: self.counters.landed.swap(0, Ordering::Relaxed),
        }
    }

    /// Attempts the logless single read-write commit path (ADR-051).
    ///
    /// An eligible overwrite commits in one conditional leaf CAS. A certified
    /// loss is replayable under the same unengaged identity; other non-landing
    /// outcomes defer to the regular locked protocol. An uncertain CAS is never
    /// replayed because it may already have committed.
    pub(super) async fn try_commit(
        &self,
        id: &TxId,
        data: &Data,
        state: &mut AttemptState,
    ) -> Result<DirectAttempt, TransError> {
        let Some(SingleRw {
            key,
            value,
            read_version,
        }) = single_rw_shape(data)
        else {
            return Ok(DirectAttempt::Locked);
        };
        self.counters.candidates.fetch_add(1, Ordering::Relaxed);
        let inline = self.inline_policy;
        // Reject values no partition could admit before routing; aggregate
        // pressure from other keys needs the folded leaf (ADR-056).
        if !inline.admits_value(value.len()) {
            return Ok(DirectAttempt::Locked);
        }
        let raw_key = key.key().to_vec();
        let Predecessor { leaf_path, writer } = match self
            .single_rw_predecessor(&key, read_version.as_ref())
            .await?
        {
            Ok(predecessor) => predecessor,
            // A read superseded before anything was staged is the second
            // certified body-replay case (ADR-053).
            Err(Ineligible::Replay) => return Ok(DirectAttempt::Replay),
            Err(Ineligible::Locked) => return Ok(DirectAttempt::Locked),
        };

        let resolver = Arc::new(DirectCommitResolver {
            id: id.clone(),
            raw_key,
            leaf_path: leaf_path.clone(),
            key,
            value,
            read_version,
            inline,
            split_hints: self.split_hints.clone(),
        });
        let outcome = self
            .coord
            .submit_shard(&leaf_path, id, resolver, Requirement::Any)
            .await?;
        match outcome {
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            }) => {
                self.counters.landed.fetch_add(1, Ordering::Relaxed);
                state.commit();
                // The predecessor lost its reference, so it may now be
                // collectable. Only a hint: it was resolved before the CAS, and
                // a logless predecessor has no object to collect at all.
                self.gc.schedule_tx_cleanup(writer);
                Ok(DirectAttempt::Committed)
            }
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::InDoubt(msg),
                ..
            }) => Err(TransError::Storage(StorageError::Unavailable(msg))),
            // The round certified that this read-modify-write staged nothing
            // durable, so its body is reevaluated instead of publishing a holder
            // merely because it shared a coordinator round (ADR-053).
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Replay,
                ..
            }) => Ok(DirectAttempt::Replay),
            // These outcomes staged nothing either, but none of them certifies
            // the replay case, so the locked protocol takes over under the same
            // id: the entry moved or is genuinely contended (`Moved`), the inline
            // node did not fit or the round was exhausted (`Conflict`), a split
            // moved the key (`Reroute`), or a shutdown ran no CAS at all
            // (`None`).
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Moved | FoldOutcome::Conflict | FoldOutcome::Reroute,
                ..
            })
            | None => Ok(DirectAttempt::Locked),
            Some(_) => Err(TransError::other(
                "direct commit produced a non-commit outcome",
            )),
        }
    }

    /// Resolves the committed predecessor and the leaf owning the key.
    ///
    /// Resolution uses the transaction body's cached shard (`Any`). A stale
    /// snapshot remains safe because a missed conditional CAS invalidates it and
    /// re-folds over the winning state.
    async fn single_rw_predecessor(
        &self,
        key: &KeyRef,
        read_version: Option<&TxId>,
    ) -> Result<Result<Predecessor, Ineligible>, TransError> {
        let (lock_state, locator) = self
            .resolver
            .resolve_key_holders(key, None, Requirement::Any)
            .await?;
        if locator
            .node()
            .is_some_and(|node| node.structural_gate().lock_type() == LockType::Write)
        {
            return Ok(Err(Ineligible::Locked));
        }
        Ok(
            eligible_writer(&lock_state, read_version).map(|writer| Predecessor {
                // The leaf that owns this key comes from descent (ADR-031), so
                // the fold targets it directly instead of recomputing a shard.
                leaf_path: locator.path,
                writer,
            }),
        )
    }
}

/// Commits an eligible single read-write transaction in one conditional leaf
/// CAS (ADR-051): it publishes `Inline { writer, value }` over the resolved
/// predecessor, installing no lock, writing no transaction object, and leaving
/// nothing to write back. The CAS *is* the commit point, so the staged entry is
/// the commit's only record: it takes a per-round claim on the key, and it
/// declines rather than publishing a pointer to a value nothing else holds when
/// the budgets close (a leaf that cannot fit the payload).
///
/// It re-resolves eligibility on every fold and classifies its own fate.
/// `Landed` means committed; `Replay` means nothing was
/// written *and* the loss is certified, so the caller may reevaluate the
/// transaction body under the same id (ADR-053); `Moved` means nothing was
/// written but only the locked protocol can resolve the entry's state;
/// `InDoubt` means the CAS may have committed and must not be re-run.
struct DirectCommitResolver {
    id: TxId,
    raw_key: Vec<u8>,
    leaf_path: ObjectPath,
    key: KeyRef,
    value: Arc<[u8]>,
    read_version: Option<TxId>,
    inline: InlinePolicy,
    split_hints: SplitHintSink,
}

#[async_trait]
impl ShardResolver for DirectCommitResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &std::collections::BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let cur = staged.get(&self.raw_key);

        // Our exact commit marker is already published: an in-doubt CAS landed
        // (possibly under a later holder's lock), so this is an idempotent
        // success rather than a second application (ADR-051).
        if cur.is_some_and(|e| self.committed(&e.current)) {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Landed,
            });
        }

        // A structural gate or a collection-deletion fence needs the logged
        // protocol's coordination, and neither is a race the direct path can
        // arbitrate.
        if staged_locks.structural_gate().lock_type() == LockType::Write
            || staged_locks.delete_intent().is_some()
        {
            return Ok(Step::Skip {
                outcome: self.unlanded(ctx, Ineligible::Locked),
            });
        }

        let res = ctx
            .key_state
            .resolve_holders(&self.key, cur, None, ctx.requirement)
            .await?;
        if let Err(why) = eligible_writer(&res, self.read_version.as_ref()) {
            return Ok(Step::Skip {
                outcome: self.unlanded(ctx, why),
            });
        }
        // A budget the folded leaf closes is a stable property of that leaf, not
        // a race a re-run of the body can win (ADR-053).
        let other_inline_bytes = staged
            .iter()
            .filter(|(key, _)| key.as_slice() != self.raw_key.as_slice())
            .map(|(_, entry)| entry.current.inline_len())
            .sum();
        if !self.inline.admits(other_inline_bytes, self.value.len()) {
            let outcome = self.unlanded(ctx, Ineligible::Locked);
            if self.inline.admits_value(self.value.len()) {
                // Resolution runs in the coordinator worker, so this
                // best-effort observation is detached from the submitter even
                // though the coordinator has no inline-pressure policy.
                self.split_hints.observe_inline_pressure(
                    &self.leaf_path,
                    &self.raw_key,
                    self.value.len(),
                );
            }
            return Ok(Step::Skip { outcome });
        }

        // Publish the value itself as the new current state, dropping the
        // entry's holders: eligibility proved every one of them is final, so an
        // already-committed writer awaiting write-back is help-forwarded and
        // replaced here (its own write-back becomes a no-op). Leaving it in
        // place would resolve the entry *backwards* to it, behind the value
        // this CAS publishes.
        let e = ShardEntry::new(self.raw_key.clone()).with_current(CurrentState::Inline {
            writer: self.id.clone(),
            value: self.value.clone(),
        });
        Ok(Step::Stage {
            entries: vec![(self.raw_key.clone(), e)],
            locks: staged_locks.clone(),
            admission: StageAdmission::InlinePublication,
            outcome: FoldOutcome::Landed,
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
        if in_doubt {
            return FoldOutcome::InDoubt("round abandoned after in-doubt CAS".into());
        }
        // An exhausted CAS budget does not certify that this attempt staged
        // nothing durable in an earlier attempt of the round, so it is not a
        // body-replay case (ADR-053).
        FoldOutcome::Moved
    }

    fn excluded_outcome(&self, in_doubt: bool) -> FoldOutcome {
        if in_doubt {
            return FoldOutcome::InDoubt(format!(
                "direct commit for {} in-doubt: excluded after an uncertain CAS",
                self.id
            ));
        }
        // A peer claimed the key before this member folded, so it staged nothing
        // at all this round: a read-modify-write may reevaluate its body against
        // the winner rather than publish a holder (ADR-053).
        self.definitive_loss()
    }

    fn owned_keys(&self) -> Vec<&[u8]> {
        vec![self.raw_key.as_slice()]
    }

    fn logless_keys(&self) -> Vec<&[u8]> {
        vec![self.raw_key.as_slice()]
    }
}

impl DirectCommitResolver {
    /// Whether `current` is this transaction's own published commit marker.
    fn committed(&self, current: &CurrentState) -> bool {
        current.writer() == Some(&self.id) && current.inline() == Some(&self.value)
    }

    /// How to report a fold that is not publishing the commit marker. Every such
    /// reason is only evidence that the marker is *not there now*. Without an
    /// in-doubt CAS that also proves nothing was ever written; after one it
    /// cannot be told from our own commit having landed and then been
    /// superseded, so the ambiguity is irreducible and is never downgraded to a
    /// replay (ADR-051, ADR-053).
    fn unlanded(&self, ctx: &ResolveCtx<'_>, why: Ineligible) -> FoldOutcome {
        if matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true }) {
            return FoldOutcome::InDoubt(format!(
                "direct commit for {} in-doubt: marker absent after an uncertain CAS",
                self.id
            ));
        }
        match why {
            Ineligible::Replay => self.definitive_loss(),
            Ineligible::Locked => FoldOutcome::Moved,
        }
    }

    /// How to report a loss that provably staged nothing durable. Only a
    /// read-modify-write has a read-dependent computation worth reevaluating; a
    /// blind overwrite would recompute the same bytes, so it takes the locked
    /// protocol instead (ADR-053).
    fn definitive_loss(&self) -> FoldOutcome {
        match self.read_version {
            Some(_) => FoldOutcome::Replay,
            None => FoldOutcome::Moved,
        }
    }
}

/// What an attempted direct commit (ADR-051) established about its transaction,
/// so the engine can tell a certified logless loss from genuine ineligibility
/// (ADR-053). An in-doubt attempt is not represented here: it is an error,
/// because it must never be re-run.
pub(super) enum DirectAttempt {
    /// The one-CAS commit landed. The transaction is committed.
    Committed,
    /// Nothing durable was staged and the loss is certified, so the
    /// read-modify-write body is reevaluated against current state under the
    /// same, still unengaged, id.
    Replay,
    /// The attempt met state only the regular locked protocol can resolve, so it
    /// acquires and validates through the general path under the same id.
    Locked,
}

/// A transaction shaped like a single read-write overwrite: the value it puts
/// and, for a read-modify-write, the version its read observed.
struct SingleRw {
    key: KeyRef,
    value: Arc<[u8]>,
    read_version: Option<TxId>,
}

/// The predecessor a direct commit builds on and the leaf that owns its key.
struct Predecessor {
    leaf_path: ObjectPath,
    writer: TxId,
}

/// Recognizes a transaction the direct commit path can publish: exactly one put,
/// no scans, and every read of that same key and found. A delete publishes a
/// tombstone and a read that found nothing makes this a create; neither has a
/// predecessor for a direct commit to build on.
fn single_rw_shape(data: &Data) -> Option<SingleRw> {
    if data.writes.len() != 1 || !data.scans.is_empty() {
        return None;
    }
    let write = &data.writes[0];
    let WriteOp::Put(value) = &write.op else {
        return None;
    };
    let mut read_version = None;
    for r in &data.reads {
        if r.key != write.key {
            return None;
        }
        read_version = Some(r.last_writer().cloned()?);
    }
    Some(SingleRw {
        key: write.key.clone(),
        value: value.clone(),
        read_version,
    })
}

/// Why a direct attempt cannot publish over an entry, and therefore what the
/// engine may do about it (ADR-053).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ineligible {
    /// The read this write depends on is definitively superseded. Nothing
    /// durable was staged, so the read-modify-write body can be reevaluated
    /// against the winner under the same id.
    Replay,
    /// The entry holds state the direct path cannot arbitrate. Only the regular
    /// locked protocol resolves it, so replaying the body would spin.
    Locked,
}

/// Decides the effective committed writer a direct commit must build on from
/// lock-domain entry state, or why the key cannot take the direct commit CAS.
///
/// Writer resolution help-forwards a committed holder while lock coordination
/// separately classifies live conflicts. A create / put over a tombstone or a
/// read-modify-write whose read was superseded is rejected (ADR-051).
///
/// Only a superseded read is [`Ineligible::Replay`], and the checks are ordered
/// so a stronger reason wins: a key read as deleted names the same writer that
/// deleted it, so testing existence first keeps it on the locked path (ADR-053).
fn eligible_writer(
    res: &HolderResolution,
    read_version: Option<&TxId>,
) -> Result<TxId, Ineligible> {
    // A live holder is a genuine conflict: defer to the full locked path so it
    // can wound-wait. Terminal holders never reach `pending`.
    if !res.pending.is_empty() {
        return Err(Ineligible::Locked);
    }
    // The key must currently exist; a create or a put over a tombstone has no
    // predecessor value, which the direct path does not handle.
    let writer = match &res.writer {
        Some(w) if !res.deleted => w.clone(),
        _ => return Err(Ineligible::Locked),
    };
    match read_version {
        // A read-modify-write commits only if its read is still current.
        Some(rv) if rv != &writer => Err(Ineligible::Replay),
        // A blind put (no read) is last-writer-wins and always serializable.
        _ => Ok(writer),
    }
}

#[cfg(test)]
mod tests;
