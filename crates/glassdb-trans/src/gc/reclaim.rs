//! Garbage collection of finalized transaction objects by candidate-driven
//! reverse mark-sweep (ADR-022).
//!
//! In the v2 object-native layout a committed transaction object *is* the value
//! store: a key's live value lives in the object its leaf entry's
//! `current_writer` points at, and readers help-forward through it. So a
//! transaction object is **live** exactly while some leaf still references its
//! transaction identity (`current_writer` or `locked_by`). Thus, GC is a
//! reachability problem, not
//! a timer.
//!
//! A forward mark (list every leaf, union the referenced transaction identities) costs the whole
//! database per cycle. Instead each candidate `_t/` object records its own
//! back-references (its `locks ∪ writes`), so GC works **backward**: it reads a
//! batch of candidates and confirms each one dead by GET-ing only the handful of
//! leaves it names — never a database-wide scan. Useful reverse-check candidates
//! come from GC scheduling through hints and scans of
//! `{db}/_t/{a}/{b}/{encoded-txid}`. This file owns reclamation safety.
//!
//! Safety rests on the ADR-021 lease as a horizon (`is_expired`): a candidate
//! other than `Wounded` is kept within the horizon, because the non-atomic
//! reverse check can race a lock a live transaction has taken but not yet
//! published (ADR-024's lazy object materialization). Past the horizon, resolution is by status:
//! a committed object is deleted only once its complete record proves it
//! unreferenced; a dead pending one is first **wounded** so its death remains
//! pinned until its owner acknowledges retirement. Wounded effects can be
//! reclaimed immediately without deleting the marker. An acknowledged aborted
//! object is retained for the ordinary finite cleanup horizon.
//!
//! Lock reclamation flows through the leaf coordinator (ADR-029): GC
//! calls the [`crate::tlocker::Locker`]'s stateless per-object unlock methods rather than issuing
//! its own leaf/root CAS, so every mutation goes through one place.

use std::collections::BTreeSet;
use std::time::UNIX_EPOCH;

use glassdb_concurr::rt;
use glassdb_data::{LogicalKey, TxId};
use glassdb_storage::transaction::{TxCollectionOp, TxCommitStatus, TxLock, TxLog};
use glassdb_storage::{Observation, Requirement, StorageError};

use super::Gc;
use crate::error::TransError;

/// Result of a GC candidate check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GcOutcome {
    Retained,
    Deferred(std::time::Duration),
    Reclaimed,
    Progress,
}

struct Reclamation {
    complete: bool,
    changed: bool,
}

impl GcOutcome {
    fn from_progress(changed: bool) -> Self {
        if changed {
            Self::Progress
        } else {
            Self::Retained
        }
    }
}

impl Gc {
    /// The reverse liveness check for one candidate (ADR-022): read it, keep it
    /// if within the safety horizon, else resolve by status.
    pub(super) async fn check_candidate(&self, tid: &TxId) -> Result<GcOutcome, TransError> {
        // GC has no preceding transaction barrier or CAS receipt: it must
        // establish one candidate-check epoch before deciding that no durable
        // leaf still references the transaction. The same epoch is propagated
        // through routing and release so the decision is one coherent sweep.
        let candidate_check = Requirement::AtLeast(self.timeline.now());
        let observed = match self.tl.get_at(tid, candidate_check).await {
            Ok(v) => v,
            // Already reclaimed (or never existed): nothing to do.
            Err(StorageError::NotFound) => return Ok(GcOutcome::Retained),
            Err(e) => return Err(e.into()),
        };
        let log = observed
            .value()
            .ok_or_else(|| StorageError::other("transaction disappeared after a present read"))?;

        // Wounded is pinned independently of time. Cleanup is deliberately
        // repeatable because an owner operation already in flight when the
        // wound landed may publish another described effect before quiescing.
        if log.status == TxCommitStatus::Wounded {
            return self
                .reclaim_wounded(tid, log, &observed, candidate_check)
                .await;
        }

        // Within the horizon: keep. A recent pending object may be a live
        // transaction whose lock this non-atomic check has not observed yet
        // (ADR-024 materializes the object lazily, after the locks are taken);
        // a recent committed/aborted one is left for a later cycle.
        let ts = log.timestamp.unwrap_or(UNIX_EPOCH);
        if !self.mon.protocol_timing().is_expired(ts, rt::system_now()) {
            let due = ts
                + self.mon.protocol_timing().max_clock_skew()
                + self.mon.protocol_timing().pending_timeout();
            return Ok(GcOutcome::Deferred(
                due.duration_since(rt::system_now()).unwrap_or_default()
                    + std::time::Duration::from_nanos(1),
            ));
        }

        match log.status {
            TxCommitStatus::Ok => {
                self.reclaim_committed(tid, log, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Aborted => {
                self.reclaim_aborted(tid, log, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Pending => {
                self.reclaim_dead_pending(tid, log, &observed, candidate_check)
                    .await
            }
            TxCommitStatus::Unknown | TxCommitStatus::Wounded => Ok(GcOutcome::Retained),
        }
    }

    /// A committed candidate past the horizon is deleted only once its complete
    /// record proves it unreferenced. Any recorded write still pointed at by a
    /// `current_writer`, or any recorded lock still held (the commit→write-back
    /// gap), keeps it. A committed object is never pruned or force-aborted — its
    /// locks become `current_writer` through write-back, never through GC.
    async fn reclaim_committed(
        &self,
        tid: &TxId,
        log: &TxLog,
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<GcOutcome, TransError> {
        // Collection effects are independent of the transaction object's value
        // reachability. A crash after the commit point must not leave a dropped
        // collection forever merely because this same log stores a live value
        // in another collection.
        let mut changed = self
            .locker
            .collections()
            .recover_write_back(tid, &log.collection_changes, &log.locks)
            .await?;
        let dropped = log
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Drop)
            .map(|change| change.collection.clone())
            .collect::<Vec<_>>();
        let active_created = log
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Create)
            .map(|change| change.collection.clone())
            .collect::<BTreeSet<_>>();
        let unused_prepared = log
            .prepared_collections
            .iter()
            .filter(|collection| !active_created.contains(*collection))
            .cloned()
            .collect::<Vec<_>>();
        changed |= self.collection_lifecycle.reclaim(&dropped).await?;
        changed |= self.collection_lifecycle.reclaim(&unused_prepared).await?;
        if self.still_referenced(tid, log, requirement).await? {
            return Ok(GcOutcome::from_progress(changed));
        }
        let released = self.release_locks(tid, &log.locks, requirement).await?;
        changed |= released.changed;
        if !released.complete {
            return Ok(GcOutcome::from_progress(changed));
        }
        self.tl.delete(observation).await?;
        Ok(GcOutcome::Reclaimed)
    }

    /// An acknowledged aborted candidate holds no value. Past its ordinary
    /// cleanup horizon, release its recorded effects and delete it. The
    /// anti-resurrection proof is the owner's acknowledgement, not elapsed time.
    async fn reclaim_aborted(
        &self,
        tid: &TxId,
        log: &TxLog,
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<GcOutcome, TransError> {
        let reclaimed = self.cleanup_aborted_effects(tid, log, requirement).await?;
        if !reclaimed.complete {
            return Ok(GcOutcome::from_progress(reclaimed.changed));
        }
        self.tl.delete(observation).await?;
        Ok(GcOutcome::Reclaimed)
    }

    /// Reclaims effects described by an unacknowledged wound but never removes
    /// the transaction marker. Only the owner may make it GC-eligible by
    /// changing it to `Aborted`.
    async fn reclaim_wounded(
        &self,
        tid: &TxId,
        log: &TxLog,
        _observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<GcOutcome, TransError> {
        let reclaimed = self.cleanup_aborted_effects(tid, log, requirement).await?;
        Ok(GcOutcome::from_progress(reclaimed.changed))
    }

    async fn cleanup_aborted_effects(
        &self,
        tid: &TxId,
        log: &TxLog,
        requirement: Requirement,
    ) -> Result<Reclamation, TransError> {
        let mut reclaimed = self.release_locks(tid, &log.locks, requirement).await?;
        if !reclaimed.complete {
            return Ok(reclaimed);
        }
        let drops = log
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Drop)
            .map(|change| change.collection.clone())
            .collect::<Vec<_>>();
        reclaimed.changed |= self
            .collection_lifecycle
            .clear_aborted_drops(tid, &drops)
            .await?;
        reclaimed.changed |= self
            .collection_lifecycle
            .reclaim(&log.prepared_collections)
            .await?;
        Ok(reclaimed)
    }

    /// A dead pending candidate is first pinned as `Wounded` (ADR-059), never
    /// made eligible for deletion by GC. If a live owner committed or
    /// refreshed first, the CAS loses and the candidate is left alone. Otherwise
    /// known abort-owned effects are reclaimed while the wound remains durable.
    async fn reclaim_dead_pending(
        &self,
        tid: &TxId,
        log: &TxLog,
        observation: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<GcOutcome, TransError> {
        let (wounded, changed) = self.mon.try_wound_observed(tid, observation).await?;
        match wounded.status {
            TxCommitStatus::Wounded => {
                let wounded_log = wounded
                    .observation
                    .value()
                    .map_or(log, std::convert::AsRef::as_ref);
                let outcome = self
                    .reclaim_wounded(tid, wounded_log, &wounded.observation, requirement)
                    .await?;
                Ok(if changed {
                    GcOutcome::Progress
                } else {
                    outcome
                })
            }
            // Committed or refreshed first: it was alive. Leave it.
            _ => Ok(GcOutcome::Retained),
        }
    }

    /// Reports whether any entry the candidate recorded still names its transaction identity: a
    /// written key's `current_writer` or a locked key's `locked_by`. Checking
    /// only the recorded set is equivalent to scanning every leaf, because an
    /// entry can name the identity only if that transaction put it there.
    ///
    /// The recorded keys are routed to their leaves by descent
    /// ([`TreeRouter::route_keys_with_requirements`]) so each touched leaf is fetched
    /// once — a write and its write-lock name the same key, and sibling keys
    /// share a leaf, so a per-key load would re-read the same leaf several times
    /// per candidate. Each key carries the [`CheckKind`] that says which field
    /// to inspect.
    async fn still_referenced(
        &self,
        tid: &TxId,
        log: &TxLog,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let mut items: Vec<(LogicalKey, CheckKind)> = log
            .writes
            .iter()
            .map(|write| (write.key.clone(), CheckKind::Writer))
            .collect();
        for lock in &log.locks {
            if let TxLock::Entry { key, .. } = lock {
                items.push((key.clone(), CheckKind::Holder));
            }
        }

        let mut by_collection = std::collections::BTreeMap::new();
        for (key, kind) in items {
            by_collection
                .entry(key.collection().clone())
                .or_insert_with(Vec::new)
                .push((key, kind));
        }
        for items in by_collection.into_values() {
            let groups = match self
                .router
                .route_keys_with_requirements(items, requirement, requirement)
                .await
            {
                Ok(groups) => groups,
                // Reclaiming a collection removes every reference it could
                // contain; its absent tree is therefore negative evidence.
                Err(StorageError::NotFound | StorageError::StaleCollection) => continue,
                Err(error) => return Err(error.into()),
            };
            for group in groups {
                let Some(leaf) = group.node().and_then(|node| node.as_leaf()) else {
                    continue;
                };
                for (raw_key, kind) in &group.keys {
                    let referenced = match kind {
                        CheckKind::Writer => {
                            leaf.lookup(raw_key).and_then(|e| e.current.writer()) == Some(tid)
                        }
                        CheckKind::Holder => {
                            leaf.lookup(raw_key).is_some_and(|e| e.is_locked_by(tid))
                        }
                    };
                    if referenced {
                        return Ok(true);
                    }
                }
            }
        }
        if self
            .locker
            .collections()
            .is_referenced(tid, &log.locks, requirement)
            .await?
        {
            return Ok(true);
        }
        Ok(false)
    }

    /// Releases `tid` from every leaf its recorded `locks` name, grouping the
    /// paths by descent so each leaf is visited once (targeted pruning, never a
    /// whole-key-space scan). Each release flows through the locker's
    /// coordinator-backed unlock method (ADR-029) — one deduplicated fold round
    /// per object that clears `tid` and drops any entry it thereby leaves
    /// vestigial — so GC issues no leaf CAS of its own. `current_writer` is
    /// never touched.
    async fn release_locks(
        &self,
        tid: &TxId,
        locks: &[TxLock],
        requirement: Requirement,
    ) -> Result<Reclamation, TransError> {
        let mut changed = false;
        let mut key_locks: Vec<(LogicalKey, ())> = Vec::new();
        let mut leaf_paths = BTreeSet::new();
        let mut topology = BTreeSet::new();
        for lock in locks {
            match lock {
                TxLock::Entry { key, .. } => key_locks.push((key.clone(), ())),
                TxLock::Membership { leaf, .. } => {
                    leaf_paths.insert(leaf.object_path());
                }
                TxLock::Directory { .. } => {}
                TxLock::Topology { collection } => {
                    topology.insert(collection.clone());
                }
            }
        }
        let mut by_collection = std::collections::BTreeMap::new();
        for (key, ()) in key_locks {
            by_collection
                .entry(key.collection().clone())
                .or_insert_with(Vec::new)
                .push((key, ()));
        }
        for items in by_collection.into_values() {
            match self
                .router
                .route_keys_with_requirements(items, requirement, requirement)
                .await
            {
                Ok(groups) => {
                    leaf_paths.extend(groups.into_iter().map(|group| group.path().clone()));
                }
                Err(StorageError::NotFound | StorageError::StaleCollection) => {}
                Err(error) => return Err(error.into()),
            }
        }
        for path in leaf_paths {
            changed |= self.locker.keys().release_leaf(tid, &path).await?;
        }
        changed |= self.locker.collections().release(tid, locks).await?;
        for collection in topology {
            let records = self
                .structural_intents
                .list_for_participant(collection.db_root_component(), tid, requirement)
                .await?;
            if !records.is_empty() {
                return Ok(Reclamation {
                    complete: false,
                    changed,
                });
            }
            changed |= self
                .locker
                .collections()
                .release_topology_participant(&collection, tid)
                .await?;
        }
        Ok(Reclamation {
            complete: true,
            changed,
        })
    }
}

/// Which leaf-entry field a liveness check consults for a recorded key: a
/// written key is referenced while it is the entry's `current_writer`; a locked
/// key while it appears in `locked_by`. Rides along as the per-key payload of
/// [`Gc::still_referenced`]'s batched leaf load.
enum CheckKind {
    Writer,
    Holder,
}
