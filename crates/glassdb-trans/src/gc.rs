//! Instance-local GC: candidate reports, scheduling, scans, and reclamation.
//!
//! Candidates record the keys and locks that can reference their transaction
//! identity. GC checks these recorded references instead of scanning every leaf
//! (ADR-022). A committed record stays live while a reference names it.
//!
//! GC skips missing, pending, and wounded records (ADR-071). It filters
//! committed and acknowledged aborted candidates by durable status and the
//! retention period before capturing the currentness barrier of their reference
//! checks. Cached leaf contents from before eligibility cannot rule out a
//! reference (ADR-079). Pinned wounds stay until owner acknowledgement
//! (ADR-059).
//!
//! Lock reclamation uses the locker's coordinator-backed operations (ADR-029),
//! so GC does not issue its own leaf mutations.

mod budget;
mod pipeline;
mod scan;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroUsize;
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::{BoxFuture, OptionFuture};
use glassdb_backend::{ListCursor, ListLimit, WriteMeter};
use glassdb_concurr::{Background, ScanCadence, rt};
use glassdb_data::{CollectionAddress, LogicalKey, TxId};
use glassdb_storage::transaction::{
    TxCollectionOp, TxCommitStatus, TxListPage, TxLock, TxRecord, TxRecordState, TxRecordStore,
};
use glassdb_storage::{
    CurrentnessBarrier, NodeStore, Observation, Requirement, StorageError, StructuralIntentStore,
    Timeline, TreeRouter,
};
use tokio::sync::Notify;

use crate::collections::CollectionLifecycle;
use crate::error::TransError;
use crate::monitor::ProtocolTiming;
use crate::structural::TopologyMembership;
use crate::tlocker::Locker;
use budget::WriteBudget;
use pipeline::CheckPipeline;
use scan::{GcScan, PAGE_SIZE};

// The candidate capacities when the budget is small. A scan needs room for two
// pages, so that a LIST can always start.
const MIN_HINT_CAPACITY: usize = 4096;
const MIN_SCAN_CAPACITY: usize = 2 * PAGE_SIZE;
// About 10 MB of candidates of each kind (ADR-078).
const MAX_CANDIDATES: usize = 65_536;
// A safety limit for memory and backend connections. The write budget, not the
// parallelism, limits how fast GC writes (ADR-078).
pub(crate) const DEFAULT_GC_PARALLELISM: NonZeroUsize = NonZeroUsize::new(256).unwrap();
// GC raises its concurrency above this only while its checks write. Checks that
// only read, of records that stay live, need no more.
const READ_ONLY_CONCURRENCY: usize = 8;
// The default limit of reports that wait for admission.
const PENDING_HINT_CAPACITY: usize = 4096;
const ADMISSION_BATCH: usize = 64;

/// GC limits for one database instance.
///
/// Excess hints are discarded without delaying writers. GC scans can still
/// discover their transaction records and use a separate candidate budget.
#[derive(Debug, Clone, Copy)]
pub struct GcLimits {
    /// Maximum reports waiting for admission. Defaults to 4,096.
    /// At capacity, a new report replaces the oldest. Zero drops every report.
    pub max_pending_hints: usize,
    /// Maximum retained candidates from hints, including ready, deferred, and
    /// running checks. Defaults to 65,536. GC retains fewer while its write
    /// budget is small: about the hints that it can check within one
    /// retention period, and at least 4,096 or this maximum if it is lower. At
    /// capacity, new candidates are discarded; existing candidates keep their
    /// place. Zero rejects all new candidates from hints.
    pub max_hint_candidates: usize,
    /// Maximum retained candidates from GC scans. Defaults to 65,536. GC
    /// retains fewer while its write budget is small, and at least two LIST
    /// pages, also with a lower maximum, so that a scan can always continue.
    /// After the capacity drops, scans pause until the retained candidates
    /// fall below it.
    pub max_scan_candidates: usize,
    /// Writes in each second that background GC can always send. Above this
    /// rate, background GC sends at most about as many writes as the rest of
    /// the instance. Defaults to 100. The rate is at least 0.01; a NaN uses
    /// the default.
    ///
    /// An instance that only collects garbage can set a higher rate. It then
    /// competes with the transactions of other instances on the same backend.
    pub base_write_rate: f64,
}

impl Default for GcLimits {
    fn default() -> Self {
        Self {
            max_pending_hints: PENDING_HINT_CAPACITY,
            max_hint_candidates: MAX_CANDIDATES,
            max_scan_candidates: MAX_CANDIDATES,
            base_write_rate: budget::DEFAULT_BASE_RATE,
        }
    }
}

type Listing = BoxFuture<'static, (GcScan, Result<Option<Vec<TxId>>, StorageError>)>;

/// Whether a transaction's durable state permits reclamation of its effects.
enum GcEligibility {
    Ready(Observation<TxRecord>),
    Deferred(Duration),
    Retained,
    Missing,
}

/// Result of a GC candidate check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcOutcome {
    Retained,
    Missing,
    Deferred(Duration),
    Reclaimed,
    Progress,
}

/// Counts of the transaction records that one full GC checked.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Records that the full GC deleted.
    pub deleted: u64,
    /// Listed records that were already deleted when the full GC checked them.
    pub missing: u64,
    /// Records whose check reclaimed some effects but kept the record. A later
    /// check can delete them.
    pub reclaimed_effects: u64,
    /// Records that wait for their retention period.
    pub deferred: u64,
    /// Time from the end of the full GC until the first deferred record
    /// becomes eligible. `None` when no record is deferred.
    pub retry_after: Option<Duration>,
    /// Records that stored objects still name, or that are pending or wounded.
    pub retained: u64,
    /// Checks that failed. A later full GC or background GC can repeat them.
    pub failed: u64,
}

struct Reclamation {
    complete: bool,
    changed: bool,
}

/// Which leaf-entry field a GC check consults for a recorded key: a
/// written key is referenced while it is the entry's `current_writer`; a locked
/// key while it appears in `locked_by`. Rides along as the per-key payload of
/// [`Gc::entries_referenced`]'s batched leaf load.
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
struct Candidates {
    entries: BTreeMap<TxId, Candidate>,
    limits: GcLimits,
    counters: Arc<Counters>,
    hint_capacity: usize,
    scan_capacity: usize,
    hint_delay: Duration,
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
    fn new(hint_delay: Duration, limits: GcLimits, counters: Arc<Counters>) -> Self {
        let mut candidates = Self {
            entries: BTreeMap::new(),
            limits,
            counters,
            hint_capacity: 0,
            scan_capacity: 0,
            hint_delay,
            hints: CandidateQueue::default(),
            scans: CandidateQueue::default(),
            deferred: BTreeSet::new(),
        };
        candidates.set_capacities(0.0, Duration::ZERO);
        candidates
    }

    /// Sizes the candidate queues to about the checks that `spend_limit`
    /// allows while a candidate holds its slot: the hint delay for hints, and
    /// also the deferral after progress for scans. Most checks make about one
    /// write. Candidates above a lower capacity keep their place.
    fn set_capacities(&mut self, spend_limit: f64, progress_delay: Duration) {
        // A NaN or negative product casts to zero, and a huge one saturates.
        let sized = |hold: Duration, min: usize, max: usize| {
            let wanted = (spend_limit * hold.as_secs_f64()) as usize;
            wanted.clamp(min.min(max), max)
        };
        self.hint_capacity = sized(
            self.hint_delay,
            MIN_HINT_CAPACITY,
            self.limits.max_hint_candidates,
        );
        self.scan_capacity = sized(
            self.hint_delay + progress_delay,
            MIN_SCAN_CAPACITY,
            self.limits.max_scan_candidates.max(MIN_SCAN_CAPACITY),
        );
    }

    fn admit(&mut self, tid: TxId, from_scan: bool, now: rt::Instant) {
        if let Some(candidate) = self.entries.get_mut(&tid) {
            if from_scan && !candidate.from_scan {
                self.hints.count -= 1;
                self.scans.count += 1;
                match candidate.state {
                    CandidateState::Ready => {
                        self.hints.ready.remove(&(candidate.due, tid));
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
            candidate.reported_again |= !from_scan && candidate.state == CandidateState::Running;
            return;
        }
        let capacity = if from_scan {
            self.scan_capacity
        } else {
            self.hint_capacity
        };
        let queue = self.queue_mut(from_scan);
        if queue.count >= capacity {
            self.counters.drop_candidate(from_scan);
            return;
        }
        queue.count += 1;
        let delay = if from_scan {
            Duration::ZERO
        } else {
            self.hint_delay
        };
        let due = now + delay;
        let state = if delay.is_zero() {
            self.queue_mut(from_scan).ready.insert((due, tid));
            CandidateState::Ready
        } else {
            self.deferred.insert((due, tid));
            CandidateState::Deferred
        };
        self.entries.insert(
            tid,
            Candidate {
                due,
                from_scan,
                state,
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
        self.deferred.insert((due, tid));
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

    /// Publishes the GC backlog. `waiting` and `in_flight` count candidates
    /// that left the ready queues: those that wait for a slot or an epoch hold
    /// no backend operation, so they count as ready.
    fn record_backlog(&self, waiting: usize, in_flight: usize) {
        let counters = &self.counters;
        counters
            .ready
            .store((self.ready_len() + waiting) as u64, Ordering::Relaxed);
        counters
            .waiting
            .store(self.deferred.len() as u64, Ordering::Relaxed);
        counters
            .in_flight
            .store(in_flight as u64, Ordering::Relaxed);
        *counters.oldest_ready.lock().unwrap() = self.oldest_ready();
    }
}

/// Owns GC scheduling and reclamation without delaying candidate producers.
#[derive(Clone)]
pub(crate) struct Gc {
    tx_records: TxRecordStore,
    structural_intents: StructuralIntentStore,
    collection_lifecycle: CollectionLifecycle,
    router: TreeRouter,
    locker: Locker,
    topology: TopologyMembership,
    timing: ProtocolTiming,
    timeline: Timeline,
    hints: GcHints,
    max_checks: NonZeroUsize,
    writes: Option<WriteMeter>,
    // Where this clone counts its estimated writes. Background GC spends
    // them from its budget; full GCs count them only so that GC earns no
    // budget from them.
    own_writes: Arc<AtomicU64>,
}

impl Gc {
    /// Creates GC with bounded candidate admission and adaptive scans. The
    /// write budget of background GC follows the writes that `writes` counts,
    /// and allows only the base rate of the hint limits without a meter.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx_records: TxRecordStore,
        nodes: NodeStore,
        structural_intents: StructuralIntentStore,
        timeline: Timeline,
        locker: Locker,
        topology: TopologyMembership,
        collection_lifecycle: CollectionLifecycle,
        timing: ProtocolTiming,
        hints: GcHints,
        max_checks: NonZeroUsize,
        writes: Option<WriteMeter>,
    ) -> Self {
        Self {
            tx_records,
            structural_intents,
            collection_lifecycle,
            router: TreeRouter::new(nodes, std::num::NonZeroUsize::MIN),
            locker,
            topology,
            timing,
            timeline,
            own_writes: hints.counters.background_writes.clone(),
            hints,
            max_checks,
            writes,
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

    /// Runs one full GC: checks every transaction record that one listing
    /// finds, with at most `parallelism` checks at the same time.
    pub(crate) async fn collect(&self, parallelism: NonZeroUsize) -> Result<GcReport, TransError> {
        let full_gc = Self {
            own_writes: self.hints.counters.full_gc_writes.clone(),
            ..self.clone()
        };
        full_gc.collect_inner(parallelism).await
    }

    async fn collect_inner(&self, parallelism: NonZeroUsize) -> Result<GcReport, TransError> {
        let mut retry_at = None::<rt::Instant>;
        let mut report = GcReport::default();
        let mut checks = CheckPipeline::new(self.clone(), parallelism);
        let mut listing: Option<BoxFuture<'_, _>> = Some(self.list_all(None).boxed());
        let mut cursor = None;
        let mut list_error = None;
        loop {
            // The next LIST overlaps the checks. Records whose check has not
            // started stay below about two pages, and the pipeline bounds the
            // forming epoch, which bounds memory.
            if listing.is_none()
                && checks.queued() < PAGE_SIZE
                && let Some(next) = cursor.take()
            {
                listing = Some(self.list_all(Some(next)).boxed());
            }
            if listing.is_none() && checks.is_idle() {
                break;
            }
            tokio::select! {
                page = async { listing.as_mut().unwrap().await }, if listing.is_some() => {
                    listing = None;
                    match page {
                        Ok(page) => {
                            page.ids.into_iter().for_each(|tid| checks.push(tid));
                            cursor = page.next;
                        }
                        // Finish the checks that started, so that their effects
                        // are counted, then report the failed LIST.
                        Err(error) => list_error = Some(error),
                    }
                    if cursor.is_none() {
                        checks.flush();
                    }
                }
                Some((tid, outcome)) = checks.next(), if !checks.is_idle() => {
                    let counters = &self.hints.counters;
                    counters.count_check(&outcome);
                    match outcome {
                        Ok(GcOutcome::Reclaimed) => {
                            report.deleted += 1;
                            counters.progress.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(GcOutcome::Progress) => {
                            report.reclaimed_effects += 1;
                            counters.progress.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(GcOutcome::Missing) => report.missing += 1,
                        Ok(GcOutcome::Retained) => report.retained += 1,
                        Ok(GcOutcome::Deferred(delay)) => {
                            report.deferred += 1;
                            let due = rt::Instant::now() + delay;
                            retry_at = Some(retry_at.map_or(due, |at| at.min(due)));
                        }
                        Err(error) => {
                            tracing::debug!(tx = %tid, error = %error, "full GC check failed");
                            report.failed += 1;
                            counters.failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
        if let Some(error) = list_error {
            return Err(error);
        }
        report.retry_after = retry_at.map(|at| at.saturating_duration_since(rt::Instant::now()));
        Ok(report)
    }

    async fn run(self) {
        let mut scheduler = Scheduler::new(&self);
        loop {
            let now = rt::Instant::now();
            let local_work = scheduler.admit_candidates(&self.hints, now);
            scheduler.start_checks(&self, now);
            let deadline = scheduler.deadline(now);
            scheduler.start_listing(now);
            scheduler
                .candidates
                .record_backlog(scheduler.checks.waiting(), scheduler.checks.in_flight());
            scheduler.record_budget();
            tokio::select! {
                biased;
                Some((scan, result)) = &mut scheduler.listing, if scheduler.scan.is_none() => {
                    scheduler.complete_listing(scan, result);
                }
                Some((tid, result)) = scheduler.checks.next(), if !scheduler.checks.is_idle() => {
                    scheduler.complete_check(tid, result);
                }
                _ = std::future::ready(()), if local_work => {}
                _ = self.hints.wake.notified() => {}
                _ = rt::sleep(deadline.saturating_duration_since(rt::Instant::now())) => {}
            }
            // GC admission must yield to transaction tasks even on an in-memory backend.
            rt::yield_now().await;
        }
    }

    /// Lists one page of all transaction records of the database.
    async fn list_all(&self, cursor: Option<ListCursor>) -> Result<TxListPage, TransError> {
        self.hints.counters.lists.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .tx_records
            .scan_transaction_ids(0, 0, cursor.as_ref(), ListLimit::new(PAGE_SIZE).unwrap())
            .await?)
    }

    /// Selects candidates whose durable state and retention period permit
    /// reclamation.
    async fn filter_candidate(
        &self,
        tid: &TxId,
        barrier: CurrentnessBarrier,
    ) -> Result<GcEligibility, TransError> {
        // GC needs exact durable evidence for reclamation. A status cached
        // without its record cannot authorize deletion, and absence must not
        // create a wound merely because a scan or hint named an ID.
        let status = self
            .tx_records
            .commit_status_at(tid, Requirement::after(barrier))
            .await?;
        match TxRecordState::try_from_observation(&status.observation)? {
            TxRecordState::Missing => return Ok(GcEligibility::Missing),
            TxRecordState::Pending | TxRecordState::Wounded => {
                return Ok(GcEligibility::Retained);
            }
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

    /// Adds to the estimate of the writes of this GC. The estimate is about
    /// one write for each object that a check changed or deleted.
    fn count_writes(&self, writes: u64) {
        self.own_writes.fetch_add(writes, Ordering::Relaxed);
    }

    /// Adds an estimate of the lock releases of a check: one write for each
    /// recorded lock, when the check changed anything. Collection deletes are
    /// counted where they happen.
    fn count_lock_writes(&self, record: &TxRecord, changed: bool) {
        if changed {
            self.count_writes(record.locks.len() as u64);
        }
    }

    /// Deletes the objects of `collections`, and reports whether it deleted
    /// any. Each deleted object is a GC write. Deletes before an error are not
    /// counted, which the rough estimate accepts.
    async fn reclaim_collections(
        &self,
        collections: &[CollectionAddress],
    ) -> Result<bool, TransError> {
        let deleted = self.collection_lifecycle.reclaim(collections).await?;
        self.count_writes(deleted);
        Ok(deleted > 0)
    }

    /// Reclaims an eligible candidate after checking its recorded references.
    async fn try_reclaim(
        &self,
        tid: &TxId,
        observed: &Observation<TxRecord>,
        barrier: CurrentnessBarrier,
    ) -> Result<GcOutcome, TransError> {
        let record = observed
            .value()
            .ok_or_else(|| StorageError::other("GC candidate has no transaction record"))?;
        match record.status {
            TxCommitStatus::Committed => {
                self.reclaim_committed(tid, record, observed, barrier).await
            }
            TxCommitStatus::Aborted => self.reclaim_aborted(tid, record, observed, barrier).await,
            TxCommitStatus::Pending | TxCommitStatus::Unknown | TxCommitStatus::Wounded => {
                Ok(GcOutcome::Retained)
            }
        }
    }

    /// A committed candidate past its retention period is deleted only once its complete
    /// record proves it unreferenced. Any recorded write still pointed at by a
    /// `current_writer`, or any recorded lock still held (the commit→write-back
    /// gap), keeps it. A committed record is never pruned or force-aborted — its
    /// locks become `current_writer` through write-back, never through GC.
    async fn reclaim_committed(
        &self,
        tid: &TxId,
        record: &TxRecord,
        observation: &Observation<TxRecord>,
        barrier: CurrentnessBarrier,
    ) -> Result<GcOutcome, TransError> {
        // Collection effects are independent of the transaction record's value
        // reachability. A crash after the commit point must not leave a dropped
        // collection forever merely because this same record stores a live value
        // in another collection.
        let removed_directories = self
            .locker
            .collections()
            .recover_write_back(
                tid,
                &record.collection_changes,
                &record.locks,
                Requirement::ANY,
            )
            .await?;
        let mut changed = !removed_directories.is_empty();
        let dropped = record
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Drop)
            .map(|change| change.collection.clone())
            .collect::<Vec<_>>();
        let active_created = record
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Create)
            .map(|change| change.collection.clone())
            .collect::<BTreeSet<_>>();
        let unused_prepared = record
            .prepared_collections
            .iter()
            .filter(|collection| !active_created.contains(*collection))
            .cloned()
            .collect::<Vec<_>>();
        changed |= self.reclaim_collections(&dropped).await?;
        changed |= self.reclaim_collections(&unused_prepared).await?;
        if self.entries_referenced(tid, record, barrier).await? {
            return Ok(GcOutcome::from_progress(changed));
        }
        // The original record's entry references are clear. Applied directory
        // write-back proves removal even after cache eviction: this
        // committed identity cannot acquire those holders again. Keep every
        // other directory and all membership/topology obligations.
        let mut remaining: Vec<_> = record
            .locks
            .iter()
            .filter(|lock| match lock {
                TxLock::Key { .. } => false,
                TxLock::Directory { collection, .. } => !removed_directories.contains(collection),
                _ => true,
            })
            .cloned()
            .collect();
        changed |= !self
            .locker
            .collections()
            .recover_write_back(
                tid,
                &record.collection_changes,
                &remaining,
                Requirement::after(barrier),
            )
            .await?
            .is_empty();
        // Successful bounded completion proves every submitted directory clear,
        // including no-ops. The returned progress set alone is not that proof.
        remaining.retain(|lock| !matches!(lock, TxLock::Directory { .. }));
        let released = self.release_locks(tid, &remaining, barrier).await?;
        changed |= released.changed;
        self.count_lock_writes(record, changed);
        if !released.complete {
            return Ok(GcOutcome::from_progress(changed));
        }
        self.tx_records.delete(observation).await?;
        self.count_writes(1);
        Ok(GcOutcome::Reclaimed)
    }

    /// An acknowledged aborted candidate holds no value. Past its retention
    /// period, release its recorded effects and delete it. The
    /// anti-resurrection proof is the owner's acknowledgement, not elapsed time.
    async fn reclaim_aborted(
        &self,
        tid: &TxId,
        record: &TxRecord,
        observation: &Observation<TxRecord>,
        barrier: CurrentnessBarrier,
    ) -> Result<GcOutcome, TransError> {
        let reclaimed = self.reclaim_aborted_effects(tid, record, barrier).await?;
        self.count_lock_writes(record, reclaimed.changed);
        if !reclaimed.complete {
            return Ok(GcOutcome::from_progress(reclaimed.changed));
        }
        self.tx_records.delete(observation).await?;
        self.count_writes(1);
        Ok(GcOutcome::Reclaimed)
    }

    async fn reclaim_aborted_effects(
        &self,
        tid: &TxId,
        record: &TxRecord,
        barrier: CurrentnessBarrier,
    ) -> Result<Reclamation, TransError> {
        let mut reclaimed = self.release_locks(tid, &record.locks, barrier).await?;
        if !reclaimed.complete {
            return Ok(reclaimed);
        }
        let drops = record
            .collection_changes
            .iter()
            .filter(|change| change.op == TxCollectionOp::Drop)
            .map(|change| change.collection.clone())
            .collect::<Vec<_>>();
        reclaimed.changed |= self
            .collection_lifecycle
            .clear_aborted_drops(tid, &drops, Requirement::after(barrier))
            .await?;
        reclaimed.changed |= self
            .reclaim_collections(&record.prepared_collections)
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
    async fn entries_referenced(
        &self,
        tid: &TxId,
        record: &TxRecord,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        let mut items: Vec<(LogicalKey, CheckKind)> = record
            .writes
            .iter()
            .map(|write| (write.key.clone(), CheckKind::Writer))
            .collect();
        for lock in &record.locks {
            if let TxLock::Key { key, .. } = lock {
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
            // Stale indexes remain usable routing hints: right links correct
            // split placement, and the terminal leaf must meet the GC bound.
            // Committed keys name collections created before commit; children
            // are created before their links are published. Neither identity
            // is reused, and published nodes remain until collection reclamation.
            // Recovery fences the split source before probing unpublished nodes.
            // Thus an old absence cannot hide a later live route.
            let groups = match self
                .router
                .route_keys_with_requirements(items, Requirement::ANY, Requirement::after(barrier))
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
        Ok(false)
    }

    /// Reclaims recorded locks while retaining unresolved structural recovery.
    async fn release_locks(
        &self,
        tid: &TxId,
        locks: &[TxLock],
        barrier: CurrentnessBarrier,
    ) -> Result<Reclamation, TransError> {
        let mut changed = self
            .locker
            .release(tid, locks, Requirement::after(barrier))
            .await?;
        let topology: BTreeSet<_> = locks
            .iter()
            .filter_map(|lock| match lock {
                TxLock::TopologyParticipant { collection } => Some(collection),
                _ => None,
            })
            .collect();
        for collection in topology {
            let records = self
                .structural_intents
                .discover_for_participant(
                    collection.db_prefix_component(),
                    tid,
                    Requirement::after(barrier),
                )
                .await?;
            if !records.is_empty() {
                return Ok(Reclamation {
                    complete: false,
                    changed,
                });
            }
            changed |= self
                .topology
                .leave(
                    collection,
                    std::slice::from_ref(tid),
                    Requirement::after(barrier),
                )
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
    checks: CheckPipeline,
    concurrency: usize,
    scan: Option<GcScan>,
    listing: OptionFuture<Listing>,
    cadence: ScanCadence,
    next_scan: rt::Instant,
    scan_useful: bool,
    scan_failed: bool,
    retry_delay: Duration,
    counters: Arc<Counters>,
    budget: WriteBudget,
    // The dropped hints at the last completed LIST.
    dropped_hints: u64,
    // The checks that completed in the current round of decisions, and those
    // of them that wrote.
    round_checks: usize,
    round_writes: usize,
}

impl Scheduler {
    fn new(gc: &Gc) -> Self {
        let retry_delay = gc.timing.pending_timeout();
        let counters = gc.hints.counters.clone();
        Self {
            candidates: Candidates::new(
                retry_delay + gc.timing.max_clock_skew(),
                gc.hints.limits,
                counters.clone(),
            ),
            scanned: VecDeque::new(),
            checks: CheckPipeline::new(gc.clone(), NonZeroUsize::MIN),
            concurrency: 1,
            scan: Some(GcScan::new(
                gc.tx_records.clone(),
                counters.clone(),
                retry_delay,
            )),
            listing: OptionFuture::default(),
            cadence: ScanCadence::new(
                (retry_delay / 16).max(Duration::from_millis(1)),
                retry_delay * 40,
            ),
            next_scan: rt::Instant::now() + retry_delay,
            scan_useful: false,
            scan_failed: false,
            retry_delay,
            budget: WriteBudget::new(
                gc.writes.clone(),
                gc.hints.limits.base_write_rate,
                gc.own_writes.clone(),
                counters.full_gc_writes.clone(),
                rt::Instant::now(),
            ),
            dropped_hints: counters.dropped_hints.load(Ordering::Relaxed),
            round_checks: 0,
            round_writes: 0,
            counters,
        }
    }

    fn admit_candidates(&mut self, hints: &GcHints, now: rt::Instant) -> bool {
        let (reports, more_hints) = hints.drain();
        for tid in reports {
            self.candidates.admit(tid, false, now);
        }
        for _ in 0..ADMISSION_BATCH {
            let Some(tid) = self.scanned.pop_front() else {
                break;
            };
            self.candidates.admit(tid, true, now);
        }
        self.candidates.promote_due(now);
        more_hints
            || !self.scanned.is_empty()
            || self.candidates.next_due().is_some_and(|due| due <= now)
    }

    fn start_checks(&mut self, gc: &Gc, now: rt::Instant) {
        let ready = self.candidates.ready_len() + self.checks.queued();
        let oldest = self
            .candidates
            .oldest_ready()
            .map(|due| now.saturating_duration_since(due))
            .unwrap_or_default();
        self.budget.refresh(now);
        self.candidates
            .set_capacities(self.budget.spend_limit(), self.retry_delay);
        let backlog = ready > self.concurrency || oldest >= self.retry_delay / 16;
        if ready == 0 && self.checks.is_idle() {
            self.concurrency = 1;
            self.round_checks = 0;
            self.round_writes = 0;
        } else if self.concurrency < READ_ONLY_CONCURRENCY {
            // Below the read-only limit, any backlog is a reason for more,
            // within the budget.
            if backlog && self.budget.allows_checks() {
                self.concurrency *= 2;
            }
        } else if self.round_checks >= self.concurrency {
            // One decision for each round of completions, so the concurrency
            // follows the share of checks that write, not each event. A budget
            // that blocks holds the concurrency: it lasts until the allowance
            // refills.
            let writing = self.round_writes * 2 >= self.round_checks;
            if !writing {
                self.concurrency = (self.concurrency / 2).max(READ_ONLY_CONCURRENCY);
            } else if backlog && self.budget.allows_checks() {
                self.concurrency *= 2;
            }
            self.round_checks = 0;
            self.round_writes = 0;
        }
        self.concurrency = self.concurrency.min(gc.max_checks.get());
        self.checks
            .set_limit(NonZeroUsize::new(self.concurrency).unwrap());
        // `take_ready` chooses between hint and scan candidates (ADR-070), so
        // only about one round of candidates leaves it.
        while self.checks.wants_candidates()
            && self.candidates.ready_len() > 0
            && self.budget.reserve_check()
        {
            let tid = self.candidates.take_ready().unwrap();
            self.checks.push(tid);
        }
        if self.candidates.ready_len() == 0 {
            self.checks.flush();
        }
    }

    /// Publishes the state of the write budget and the concurrency.
    fn record_budget(&self) {
        // The cast saturates for a very large base rate.
        self.counters
            .spend_limit
            .store(self.budget.spend_limit() as u64, Ordering::Relaxed);
        self.counters
            .concurrency
            .store(self.concurrency as u64, Ordering::Relaxed);
    }

    fn can_list(&self) -> bool {
        self.scan.is_some()
            && self.scanned.is_empty()
            && self.candidates.scans.count + PAGE_SIZE <= self.candidates.scan_capacity
    }

    fn deadline(&self, now: rt::Instant) -> rt::Instant {
        let refill = (self.candidates.ready_len() > 0)
            .then(|| self.budget.refill_at(self.retry_delay * 40))
            .flatten();
        self.candidates
            .next_due()
            .into_iter()
            .chain(refill)
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
                // A dropped hint names a record that can have GC work that GC
                // does not know about, so scans must not slow down while hints
                // are dropped (ADR-078).
                let dropped_hints = self.counters.dropped_hints.load(Ordering::Relaxed);
                self.scan_useful |= dropped_hints > self.dropped_hints;
                self.dropped_hints = dropped_hints;
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

    fn complete_check(&mut self, tid: TxId, result: Result<GcOutcome, TransError>) {
        self.counters.count_check(&result);
        self.budget.release_check();
        self.round_checks += 1;
        let mut candidate = self.candidates.complete(&tid);
        let retry = match result {
            Ok(GcOutcome::Reclaimed) => {
                self.counters.progress.fetch_add(1, Ordering::Relaxed);
                self.scan_useful |= candidate.from_scan;
                self.round_writes += 1;
                None
            }
            Ok(GcOutcome::Progress) => {
                self.counters.progress.fetch_add(1, Ordering::Relaxed);
                self.scan_useful |= candidate.from_scan;
                self.round_writes += 1;
                Some(self.retry_delay)
            }
            Ok(GcOutcome::Deferred(delay)) => Some(delay),
            Ok(GcOutcome::Retained | GcOutcome::Missing) => candidate
                .reported_again
                .then_some(self.candidates.hint_delay),
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

/// Reports GC candidates without waiting for GC or queue capacity.
#[derive(Clone, Default)]
pub(crate) struct GcHints {
    limits: GcLimits,
    queue: Arc<Mutex<VecDeque<TxId>>>,
    wake: Arc<Notify>,
    counters: Arc<Counters>,
}

impl GcHints {
    /// Creates a candidate feed with shared producer and admission limits.
    pub(crate) fn new(limits: GcLimits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }

    /// Reports one transaction record for a GC check.
    pub(crate) fn schedule(&self, tid: TxId) {
        self.schedule_all([tid]);
    }

    /// Reports transaction records for GC checks.
    pub(crate) fn schedule_all(&self, tids: impl IntoIterator<Item = TxId>) {
        for tid in tids {
            if self.limits.max_pending_hints == 0 {
                self.counters.drop_candidate(false);
                continue;
            }
            // A busy consumer must not put a writer behind the GC queue lock.
            let Ok(mut queue) = self.queue.try_lock() else {
                self.counters.drop_candidate(false);
                continue;
            };
            if queue.len() == self.limits.max_pending_hints {
                queue.pop_front();
                self.counters.drop_candidate(false);
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
    /// Transaction-record LIST requests.
    pub lists: u64,
    /// Completed GC checks of background GC and full GCs.
    pub checks: u64,
    /// Checks that found their record inside its retention period, so that
    /// they read it without a chance to reclaim it.
    pub deferred_checks: u64,
    /// The estimated writes of GC, which background GC spends from its write
    /// budget. It counts one write for each object that GC changed or deleted.
    pub writes: u64,
}

impl AddAssign for GcStats {
    fn add_assign(&mut self, rhs: Self) {
        self.dropped_candidates += rhs.dropped_candidates;
        self.failures += rhs.failures;
        self.progress += rhs.progress;
        self.lists += rhs.lists;
        self.checks += rhs.checks;
        self.deferred_checks += rhs.deferred_checks;
        self.writes += rhs.writes;
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
            checks: self.checks.saturating_sub(rhs.checks),
            deferred_checks: self.deferred_checks.saturating_sub(rhs.deferred_checks),
            writes: self.writes.saturating_sub(rhs.writes),
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
    /// The whole writes in each second that the write budget of background GC
    /// allows now.
    pub spend_limit: u64,
    /// The checks that background GC can run at the same time now.
    pub concurrency: u64,
}

#[derive(Default)]
struct Counters {
    // The hints dropped since GC started. Unlike `dropped_candidates`, a stats
    // snapshot does not reset it, so the scan cadence can measure its own
    // intervals.
    dropped_hints: AtomicU64,
    // The estimated writes of every full GC of this instance.
    full_gc_writes: Arc<AtomicU64>,
    dropped_candidates: AtomicU64,
    failures: AtomicU64,
    progress: AtomicU64,
    lists: AtomicU64,
    checks: AtomicU64,
    deferred_checks: AtomicU64,
    // The writes of `writes` that the last stats snapshot reported. The
    // estimates themselves only grow, because the write budget reads them.
    reported_writes: AtomicU64,
    // The estimated writes of background GC.
    background_writes: Arc<AtomicU64>,
    spend_limit: AtomicU64,
    concurrency: AtomicU64,
    ready: AtomicU64,
    waiting: AtomicU64,
    in_flight: AtomicU64,
    oldest_ready: Mutex<Option<rt::Instant>>,
    prefix_count: AtomicU64,
}

impl Counters {
    /// Counts a dropped candidate. Only a dropped hint is also scan demand: a
    /// dropped scan candidate is a record that the next scan pass finds again.
    fn drop_candidate(&self, from_scan: bool) {
        self.dropped_candidates.fetch_add(1, Ordering::Relaxed);
        if !from_scan {
            self.dropped_hints.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn take(&self) -> GcStats {
        GcStats {
            dropped_candidates: self.dropped_candidates.swap(0, Ordering::Relaxed),
            failures: self.failures.swap(0, Ordering::Relaxed),
            progress: self.progress.swap(0, Ordering::Relaxed),
            lists: self.lists.swap(0, Ordering::Relaxed),
            checks: self.checks.swap(0, Ordering::Relaxed),
            deferred_checks: self.deferred_checks.swap(0, Ordering::Relaxed),
            writes: {
                let writes = self.background_writes.load(Ordering::Relaxed)
                    + self.full_gc_writes.load(Ordering::Relaxed);
                writes.saturating_sub(self.reported_writes.swap(writes, Ordering::Relaxed))
            },
        }
    }

    /// Counts a completed check.
    fn count_check(&self, outcome: &Result<GcOutcome, TransError>) {
        self.checks.fetch_add(1, Ordering::Relaxed);
        if matches!(outcome, Ok(GcOutcome::Deferred(_))) {
            self.deferred_checks.fetch_add(1, Ordering::Relaxed);
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
            spend_limit: self.spend_limit.load(Ordering::Relaxed),
            concurrency: self.concurrency.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests;
