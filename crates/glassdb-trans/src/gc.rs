//! Instance-local GC: candidate reports, scheduling, scans, and reclamation.
//!
//! Candidates record the keys and locks that can reference their transaction
//! identity. GC checks these recorded references instead of scanning every leaf
//! (ADR-022). A committed log stays live while a reference names it.
//!
//! GC skips missing and pending logs (ADR-071). It filters final candidates by
//! durable status and the safety horizon before capturing a fresh requirement
//! for reference checks. Cached absence from before eligibility cannot authorize
//! deletion. Wounded markers stay pinned until owner acknowledgement (ADR-059).
//!
//! Lock reclamation uses the locker's coordinator-backed operations (ADR-029),
//! so GC does not issue its own leaf mutations.

mod scan;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{
    FutureExt,
    future::{BoxFuture, OptionFuture},
};
use glassdb_concurr::{Background, ScanCadence, map_all_bounded, rt};
use glassdb_data::{LogicalKey, TxId};
use glassdb_storage::{
    NodeStore, Observation, Requirement, StorageError, StructuralIntentStore, Timeline, TreeRouter,
    transaction::{TLogger, TxCollectionOp, TxCommitStatus, TxLock, TxLog, TxRecordState},
};
use tokio::sync::Notify;

use crate::collections::CollectionLifecycle;
use crate::error::TransError;
use crate::monitor::ProtocolTiming;
use crate::tlocker::Locker;
use scan::{GcScan, PAGE_SIZE};

const HINT_CAPACITY: usize = 4096;
const SCAN_CAPACITY: usize = 2 * PAGE_SIZE;
const MAX_CHECKS: usize = 8;
const ADMISSION_BATCH: usize = 64;

type Checks = BoxFuture<'static, Vec<(TxId, Result<GcOutcome, TransError>)>>;
type Listing = BoxFuture<'static, (GcScan, Result<Option<Vec<TxId>>, StorageError>)>;

/// Whether a transaction's durable state permits reclamation of its effects.
enum GcEligibility {
    Ready(Observation<TxLog>),
    Deferred(Duration),
    Retained,
}

/// Result of a GC candidate check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcOutcome {
    Retained,
    Deferred(Duration),
    Reclaimed,
    Progress,
}

struct Reclamation {
    complete: bool,
    changed: bool,
}

/// Which leaf-entry field a liveness check consults for a recorded key: a
/// written key is referenced while it is the entry's `current_writer`; a locked
/// key while it appears in `locked_by`. Rides along as the per-key payload of
/// [`Gc::still_referenced`]'s batched leaf load.
enum CheckKind {
    Writer,
    Holder,
}

struct Candidate {
    due: rt::Instant,
    from_scan: bool,
    state: CandidateState,
    reported_again: bool,
    failures: u32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CandidateState {
    Ready,
    Deferred,
    Running,
}

/// Bounds local work and preserves scan capacity under a stream of hints.
#[derive(Default)]
struct Candidates {
    entries: BTreeMap<TxId, Candidate>,
    hints: CandidateQueue,
    scans: CandidateQueue,
    deferred: BTreeSet<(rt::Instant, TxId)>,
}

#[derive(Default)]
struct CandidateQueue {
    ready: BTreeSet<(rt::Instant, TxId)>,
    count: usize,
    running: usize,
}

impl Candidates {
    fn admit(&mut self, tid: TxId, from_scan: bool, now: rt::Instant, counters: &Counters) {
        if let Some(candidate) = self.entries.get_mut(&tid) {
            if from_scan && !candidate.from_scan {
                self.hints.count -= 1;
                self.scans.count += 1;
                match candidate.state {
                    CandidateState::Ready => {
                        self.hints.ready.remove(&(candidate.due, tid.clone()));
                        self.scans.ready.insert((candidate.due, tid));
                    }
                    CandidateState::Running => {
                        self.hints.running -= 1;
                        self.scans.running += 1;
                    }
                    CandidateState::Deferred => {}
                }
                candidate.from_scan = true;
            }
            candidate.reported_again |= candidate.state == CandidateState::Running;
            return;
        }
        let queue = self.queue_mut(from_scan);
        let capacity = if from_scan {
            SCAN_CAPACITY
        } else {
            HINT_CAPACITY
        };
        if queue.count >= capacity {
            counters.dropped_candidates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        queue.count += 1;
        queue.ready.insert((now, tid.clone()));
        self.entries.insert(
            tid,
            Candidate {
                due: now,
                from_scan,
                state: CandidateState::Ready,
                reported_again: false,
                failures: 0,
            },
        );
    }

    fn promote_due(&mut self, now: rt::Instant) {
        for _ in 0..ADMISSION_BATCH {
            if !self.next_due().is_some_and(|due| due <= now) {
                break;
            }
            let (due, tid) = self.deferred.pop_first().unwrap();
            let candidate = self.entries.get_mut(&tid).unwrap();
            candidate.state = CandidateState::Ready;
            let from_scan = candidate.from_scan;
            self.queue_mut(from_scan).ready.insert((due, tid));
        }
    }

    fn take_ready(&mut self) -> Option<TxId> {
        // Keep a check slot available for scans despite continuous hint traffic.
        let from_scan = match (self.hints.ready.first(), self.scans.ready.first()) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(hint), Some(scan)) => self.scans.running == 0 || scan < hint,
        };
        let queue = self.queue_mut(from_scan);
        let (_, tid) = queue.ready.pop_first()?;
        queue.running += 1;
        self.entries.get_mut(&tid).unwrap().state = CandidateState::Running;
        Some(tid)
    }

    fn complete(&mut self, tid: &TxId) -> Candidate {
        let candidate = self.entries.remove(tid).unwrap();
        let queue = self.queue_mut(candidate.from_scan);
        queue.count -= 1;
        queue.running -= 1;
        candidate
    }

    fn defer(&mut self, tid: TxId, mut candidate: Candidate, due: rt::Instant) {
        candidate.state = CandidateState::Deferred;
        candidate.reported_again = false;
        candidate.due = due;
        self.queue_mut(candidate.from_scan).count += 1;
        self.deferred.insert((due, tid.clone()));
        self.entries.insert(tid, candidate);
    }

    fn ready_len(&self) -> usize {
        self.hints.ready.len() + self.scans.ready.len()
    }

    fn oldest_ready(&self) -> Option<rt::Instant> {
        self.hints
            .ready
            .first()
            .into_iter()
            .chain(self.scans.ready.first())
            .map(|(due, _)| *due)
            .min()
    }

    fn next_due(&self) -> Option<rt::Instant> {
        self.deferred.first().map(|(due, _)| *due)
    }

    fn queue_mut(&mut self, from_scan: bool) -> &mut CandidateQueue {
        if from_scan {
            &mut self.scans
        } else {
            &mut self.hints
        }
    }

    fn record_backlog(&self, counters: &Counters) {
        counters
            .ready
            .store(self.ready_len() as u64, Ordering::Relaxed);
        counters
            .waiting
            .store(self.deferred.len() as u64, Ordering::Relaxed);
        counters.in_flight.store(
            (self.hints.running + self.scans.running) as u64,
            Ordering::Relaxed,
        );
        *counters.oldest_ready.lock().unwrap() = self.oldest_ready();
    }
}

/// Owns GC scheduling and reclamation without delaying candidate producers.
#[derive(Clone)]
pub(crate) struct Gc {
    tl: TLogger,
    structural_intents: StructuralIntentStore,
    collection_lifecycle: CollectionLifecycle,
    router: TreeRouter,
    locker: Locker,
    timing: ProtocolTiming,
    timeline: Timeline,
    hints: GcHints,
}

impl Gc {
    /// Creates GC with bounded candidate admission and adaptive scans.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tl: TLogger,
        nodes: NodeStore,
        structural_intents: StructuralIntentStore,
        timeline: Timeline,
        locker: Locker,
        collection_lifecycle: CollectionLifecycle,
        timing: ProtocolTiming,
        hints: GcHints,
    ) -> Self {
        Self {
            tl,
            structural_intents,
            collection_lifecycle,
            router: TreeRouter::new(nodes, std::num::NonZeroUsize::MIN),
            locker,
            timing,
            timeline,
            hints,
        }
    }

    pub(crate) fn stats_and_reset(&self) -> GcStats {
        self.hints.counters.take()
    }

    pub(crate) fn diagnostics(&self) -> GcDiagnostics {
        self.hints.counters.diagnostics()
    }

    /// Starts background GC checks and scans.
    pub(crate) fn start(&self, background: &Background) {
        background.spawn(self.clone().run());
    }

    async fn run(self) {
        let mut scheduler = Scheduler::new(&self);
        loop {
            let now = rt::Instant::now();
            let local_work = scheduler.admit_candidates(&self.hints, now);
            scheduler.start_checks(&self, now);
            let deadline = scheduler.deadline(now);
            scheduler.start_listing(now);
            scheduler.candidates.record_backlog(&scheduler.counters);
            tokio::select! {
                biased;
                Some((scan, result)) = &mut scheduler.listing, if scheduler.scan.is_none() => {
                    scheduler.complete_listing(scan, result);
                }
                Some(completed) = &mut scheduler.checks, if scheduler.checking => {
                    scheduler.complete_checks(completed);
                }
                _ = std::future::ready(()), if local_work => {}
                _ = self.hints.wake.notified() => {}
                _ = rt::sleep(deadline.saturating_duration_since(rt::Instant::now())) => {}
            }
            // GC admission must yield to transaction tasks even on an in-memory backend.
            rt::yield_now().await;
        }
    }

    async fn check_batch(
        self,
        tids: Vec<TxId>,
        limit: NonZeroUsize,
    ) -> Vec<(TxId, Result<GcOutcome, TransError>)> {
        let gc = &self;
        let status_requirement = Requirement::AtLeast(self.timeline.now());
        let filtered = map_all_bounded(tids, limit, |tid| async move {
            let result = gc.filter_candidate(&tid, status_requirement).await;
            (tid, result)
        })
        .await;
        // All candidates must be eligible before this shared reference bound.
        let requirement = Requirement::AtLeast(self.timeline.now());
        map_all_bounded(filtered, limit, |(tid, result)| async move {
            let result = match result {
                Ok(GcEligibility::Ready(observation)) => {
                    gc.try_reclaim(&tid, &observation, requirement).await
                }
                Ok(GcEligibility::Deferred(delay)) => Ok(GcOutcome::Deferred(delay)),
                Ok(GcEligibility::Retained) => Ok(GcOutcome::Retained),
                Err(error) => Err(error),
            };
            (tid, result)
        })
        .await
    }

    /// Selects candidates whose durable state and safety horizon permit reclamation.
    async fn filter_candidate(
        &self,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<GcEligibility, TransError> {
        // GC needs exact durable evidence for reclamation. A status cached
        // without its log cannot authorize deletion, and absence must not
        // create a wound marker merely because a scan or hint named an ID.
        let status = self.tl.commit_status_at(tid, requirement).await?;
        match TxRecordState::try_from_observation(&status.observation)? {
            TxRecordState::Missing | TxRecordState::Pending => return Ok(GcEligibility::Retained),
            TxRecordState::Wounded => return Ok(GcEligibility::Ready(status.observation)),
            TxRecordState::Committed | TxRecordState::Aborted => {}
        }
        let now = rt::system_now();
        if !self.timing.is_expired(status.last_update, now) {
            let due =
                status.last_update + self.timing.max_clock_skew() + self.timing.pending_timeout();
            return Ok(GcEligibility::Deferred(
                due.duration_since(now).unwrap_or_default() + Duration::from_nanos(1),
            ));
        }
        Ok(GcEligibility::Ready(status.observation))
    }

    /// Reclaims an eligible candidate after checking its recorded references.
    async fn try_reclaim(
        &self,
        tid: &TxId,
        observed: &Observation<TxLog>,
        requirement: Requirement,
    ) -> Result<GcOutcome, TransError> {
        let log = observed
            .value()
            .ok_or_else(|| StorageError::other("GC candidate has no transaction log"))?;
        match log.status {
            TxCommitStatus::Ok => {
                self.reclaim_committed(tid, log, observed, requirement)
                    .await
            }
            TxCommitStatus::Aborted => self.reclaim_aborted(tid, log, observed, requirement).await,
            TxCommitStatus::Wounded => self.reclaim_wounded(tid, log, observed, requirement).await,
            TxCommitStatus::Pending | TxCommitStatus::Unknown => Ok(GcOutcome::Retained),
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
        // The liveness check already ruled out every recorded entry lock.
        // Membership and topology effects still need their own cleanup.
        let remaining: Vec<_> = log
            .locks
            .iter()
            .filter(|lock| !matches!(lock, TxLock::Entry { .. }))
            .cloned()
            .collect();
        let released = self.release_locks(tid, &remaining, requirement).await?;
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

impl GcOutcome {
    fn from_progress(changed: bool) -> Self {
        if changed {
            Self::Progress
        } else {
            Self::Retained
        }
    }
}

/// Keeps scheduling state local to one running GC loop.
struct Scheduler {
    candidates: Candidates,
    scanned: VecDeque<TxId>,
    checks: OptionFuture<Checks>,
    checking: bool,
    concurrency: usize,
    scan: Option<GcScan>,
    listing: OptionFuture<Listing>,
    cadence: ScanCadence,
    next_scan: rt::Instant,
    scan_useful: bool,
    scan_failed: bool,
    retry_delay: Duration,
    counters: Arc<Counters>,
}

impl Scheduler {
    fn new(gc: &Gc) -> Self {
        let retry_delay = gc.timing.pending_timeout();
        let counters = gc.hints.counters.clone();
        Self {
            candidates: Candidates::default(),
            scanned: VecDeque::new(),
            checks: OptionFuture::default(),
            checking: false,
            concurrency: 1,
            scan: Some(GcScan::new(gc.tl.clone(), counters.clone(), retry_delay)),
            listing: OptionFuture::default(),
            cadence: ScanCadence::new(
                (retry_delay / 16).max(Duration::from_millis(1)),
                retry_delay * 40,
            ),
            next_scan: rt::Instant::now() + retry_delay,
            scan_useful: false,
            scan_failed: false,
            retry_delay,
            counters,
        }
    }

    fn admit_candidates(&mut self, hints: &GcHints, now: rt::Instant) -> bool {
        let (reports, more_hints) = hints.drain();
        for tid in reports {
            self.candidates.admit(tid, false, now, &self.counters);
        }
        for _ in 0..ADMISSION_BATCH {
            let Some(tid) = self.scanned.pop_front() else {
                break;
            };
            self.candidates.admit(tid, true, now, &self.counters);
        }
        self.candidates.promote_due(now);
        more_hints
            || !self.scanned.is_empty()
            || self.candidates.next_due().is_some_and(|due| due <= now)
    }

    fn start_checks(&mut self, gc: &Gc, now: rt::Instant) {
        let ready = self.candidates.ready_len();
        let oldest = self
            .candidates
            .oldest_ready()
            .map(|due| now.saturating_duration_since(due))
            .unwrap_or_default();
        if ready > self.concurrency || oldest >= self.retry_delay / 16 {
            self.concurrency = (self.concurrency * 2).min(MAX_CHECKS);
        } else if ready == 0 && !self.checking {
            self.concurrency = 1;
        }
        if self.checking {
            return;
        }
        let batch: Vec<_> = std::iter::from_fn(|| self.candidates.take_ready())
            .take(self.concurrency)
            .collect();
        if !batch.is_empty() {
            self.checking = true;
            self.checks = Some(
                gc.clone()
                    .check_batch(batch, NonZeroUsize::new(self.concurrency).unwrap())
                    .boxed(),
            )
            .into();
        }
    }

    fn can_list(&self) -> bool {
        self.scan.is_some()
            && self.scanned.is_empty()
            && self.candidates.scans.count + PAGE_SIZE <= SCAN_CAPACITY
    }

    fn deadline(&self, now: rt::Instant) -> rt::Instant {
        self.candidates
            .next_due()
            .into_iter()
            .chain(self.can_list().then_some(self.next_scan))
            .min()
            .unwrap_or(now + self.retry_delay * 40)
            .max(now + Duration::from_millis(1))
    }

    fn start_listing(&mut self, now: rt::Instant) {
        if !self.can_list() || now < self.next_scan {
            return;
        }
        let mut scan = self.scan.take().unwrap();
        self.listing = Some(
            async move {
                let result = scan.turn().await;
                (scan, result)
            }
            .boxed(),
        )
        .into();
    }

    fn complete_listing(&mut self, scan: GcScan, result: Result<Option<Vec<TxId>>, StorageError>) {
        self.listing = None.into();
        let delay = match result {
            Ok(Some(ids)) => {
                if !self.scan_failed {
                    self.cadence.observe(self.scan_useful);
                }
                self.scan_useful = false;
                self.scan_failed = false;
                self.scanned.extend(ids);
                let delay = self.cadence.delay();
                if scan.has_continuation() {
                    delay.min(self.retry_delay)
                } else {
                    delay
                }
            }
            Ok(None) => scan
                .retry_at()
                .map(|at| at.saturating_duration_since(rt::Instant::now()))
                .unwrap_or(self.retry_delay),
            Err(error) => {
                tracing::debug!(error = %error, "GC scan deferred");
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                self.retry_delay
            }
        };
        self.next_scan = rt::Instant::now() + delay;
        self.scan = Some(scan);
    }

    fn complete_checks(&mut self, completed: Vec<(TxId, Result<GcOutcome, TransError>)>) {
        self.checking = false;
        self.checks = None.into();
        for (tid, result) in completed {
            let mut candidate = self.candidates.complete(&tid);
            let retry = match result {
                Ok(GcOutcome::Reclaimed) => {
                    self.counters.progress.fetch_add(1, Ordering::Relaxed);
                    self.scan_useful |= candidate.from_scan;
                    None
                }
                Ok(GcOutcome::Progress) => {
                    self.counters.progress.fetch_add(1, Ordering::Relaxed);
                    self.scan_useful |= candidate.from_scan;
                    Some(self.retry_delay)
                }
                Ok(GcOutcome::Deferred(delay)) => Some(delay),
                Ok(GcOutcome::Retained) => candidate.reported_again.then_some(self.retry_delay),
                Err(error) => {
                    tracing::debug!(tx = %tid, error = %error, "GC candidate check deferred");
                    self.counters.failures.fetch_add(1, Ordering::Relaxed);
                    self.scan_failed |= candidate.from_scan;
                    candidate.failures = candidate.failures.saturating_add(1);
                    Some(self.retry_delay * (1u32 << candidate.failures.min(4)))
                }
            };
            if let Some(delay) = retry {
                self.candidates
                    .defer(tid, candidate, rt::Instant::now() + delay);
            }
        }
    }
}

/// Reports GC candidates without waiting for GC or queue capacity.
#[derive(Clone, Default)]
pub(crate) struct GcHints {
    queue: Arc<Mutex<VecDeque<TxId>>>,
    wake: Arc<Notify>,
    counters: Arc<Counters>,
}

impl GcHints {
    /// Reports one transaction object for a reverse liveness check.
    pub(crate) fn schedule(&self, tid: TxId) {
        self.schedule_all([tid]);
    }

    /// Reports transaction objects for reverse liveness checks.
    pub(crate) fn schedule_all(&self, tids: impl IntoIterator<Item = TxId>) {
        for tid in tids {
            // A busy consumer must not put a writer behind the GC queue lock.
            let Ok(mut queue) = self.queue.try_lock() else {
                self.counters
                    .dropped_candidates
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if queue.len() == HINT_CAPACITY {
                queue.pop_front();
                self.counters
                    .dropped_candidates
                    .fetch_add(1, Ordering::Relaxed);
            }
            let wake = queue.is_empty();
            queue.push_back(tid);
            drop(queue);
            if wake {
                self.wake.notify_one();
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> Vec<TxId> {
        self.queue.lock().unwrap().iter().cloned().collect()
    }

    fn drain(&self) -> (Vec<TxId>, bool) {
        let mut queue = self.queue.lock().unwrap();
        let count = queue.len().min(ADMISSION_BATCH);
        let reports = queue.drain(..count).collect();
        (reports, !queue.is_empty())
    }
}

/// GC activity during one snapshot interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcStats {
    /// GC candidate reports discarded at either buffer limit or on producer contention.
    pub dropped_candidates: u64,
    /// Failed candidate checks or LIST requests.
    pub failures: u64,
    /// Checks that reclaimed resources or advanced recovery, including transaction deletion.
    /// Concurrent deletions can count as progress in more than one Database instance.
    pub progress: u64,
    /// Transaction-object LIST requests.
    pub lists: u64,
}

impl AddAssign for GcStats {
    fn add_assign(&mut self, rhs: Self) {
        self.dropped_candidates += rhs.dropped_candidates;
        self.failures += rhs.failures;
        self.progress += rhs.progress;
        self.lists += rhs.lists;
    }
}

impl Sub for GcStats {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self {
            dropped_candidates: self
                .dropped_candidates
                .saturating_sub(rhs.dropped_candidates),
            failures: self.failures.saturating_sub(rhs.failures),
            progress: self.progress.saturating_sub(rhs.progress),
            lists: self.lists.saturating_sub(rhs.lists),
        }
    }
}

/// Current local GC work; retained objects are not backlog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcDiagnostics {
    /// Known candidates whose checks are due.
    pub ready: u64,
    /// Known candidates waiting for their next permitted check.
    pub deferred: u64,
    /// Candidate checks currently running.
    pub in_flight: u64,
    /// Age of the oldest ready check, measured from its due time.
    pub oldest_ready_age: Duration,
    /// Prefix count used for new traversals.
    pub prefix_count: u64,
}

#[derive(Default)]
struct Counters {
    dropped_candidates: AtomicU64,
    failures: AtomicU64,
    progress: AtomicU64,
    lists: AtomicU64,
    ready: AtomicU64,
    waiting: AtomicU64,
    in_flight: AtomicU64,
    oldest_ready: Mutex<Option<rt::Instant>>,
    prefix_count: AtomicU64,
}

impl Counters {
    fn take(&self) -> GcStats {
        GcStats {
            dropped_candidates: self.dropped_candidates.swap(0, Ordering::Relaxed),
            failures: self.failures.swap(0, Ordering::Relaxed),
            progress: self.progress.swap(0, Ordering::Relaxed),
            lists: self.lists.swap(0, Ordering::Relaxed),
        }
    }

    fn diagnostics(&self) -> GcDiagnostics {
        GcDiagnostics {
            ready: self.ready.load(Ordering::Relaxed),
            deferred: self.waiting.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            oldest_ready_age: self
                .oldest_ready
                .lock()
                .unwrap()
                .map(|due| rt::Instant::now().saturating_duration_since(due))
                .unwrap_or_default(),
            prefix_count: self.prefix_count.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests;
