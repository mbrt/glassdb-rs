//! Instance-local GC: candidate reports, scheduling, scans, and reclamation.

mod reclaim;
mod scan;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use glassdb_concurr::{Background, ScanCadence, rt};
use glassdb_data::TxId;
use glassdb_storage::{
    NodeStore, StorageError, StructuralIntentStore, Timeline, TreeRouter, transaction::TLogger,
};
use tokio::sync::Notify;

use crate::collections::CollectionLifecycle;
use crate::error::TransError;
use crate::monitor::Monitor;
use crate::tlocker::Locker;
use reclaim::GcOutcome;
use scan::{GcScan, PAGE_SIZE};

const HINT_CAPACITY: usize = 4096;
const SCAN_CAPACITY: usize = 2 * PAGE_SIZE;
const MAX_CHECKS: usize = 8;
const ADMISSION_BATCH: usize = 64;

type Check = BoxFuture<'static, (TxId, Result<GcOutcome, TransError>)>;
type Listing = BoxFuture<'static, (GcScan, Result<Option<Vec<TxId>>, StorageError>)>;

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
    ready: [BTreeSet<(rt::Instant, TxId)>; 2],
    deferred: BTreeSet<(rt::Instant, TxId)>,
    counts: [usize; 2],
    running: [usize; 2],
}

impl Candidates {
    fn admit(&mut self, tid: TxId, from_scan: bool, now: rt::Instant, counters: &Counters) {
        if let Some(candidate) = self.entries.get_mut(&tid) {
            if from_scan && !candidate.from_scan {
                self.counts[0] -= 1;
                self.counts[1] += 1;
                match candidate.state {
                    CandidateState::Ready => {
                        self.ready[0].remove(&(candidate.due, tid.clone()));
                        self.ready[1].insert((candidate.due, tid));
                    }
                    CandidateState::Running => {
                        self.running[0] -= 1;
                        self.running[1] += 1;
                    }
                    CandidateState::Deferred => {}
                }
                candidate.from_scan = true;
            }
            candidate.reported_again |= candidate.state == CandidateState::Running;
            return;
        }
        let origin = usize::from(from_scan);
        let capacity = if from_scan {
            SCAN_CAPACITY
        } else {
            HINT_CAPACITY
        };
        if self.counts[origin] >= capacity {
            counters.dropped_candidates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counts[origin] += 1;
        self.ready[origin].insert((now, tid.clone()));
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
            self.ready[usize::from(candidate.from_scan)].insert((due, tid));
        }
    }

    fn take_ready(&mut self) -> Option<TxId> {
        // Keep a check slot available for scans despite continuous hint traffic.
        let origin = if self.running[1] == 0 && !self.ready[1].is_empty() {
            1
        } else {
            (0..2)
                .filter(|&i| !self.ready[i].is_empty())
                .min_by_key(|&i| self.ready[i].first())
                .unwrap_or(0)
        };
        let (_, tid) = self.ready[origin].pop_first()?;
        self.entries.get_mut(&tid).unwrap().state = CandidateState::Running;
        self.running[origin] += 1;
        Some(tid)
    }

    fn complete(&mut self, tid: &TxId) -> Candidate {
        let candidate = self.entries.remove(tid).unwrap();
        let origin = usize::from(candidate.from_scan);
        self.counts[origin] -= 1;
        self.running[origin] -= 1;
        candidate
    }

    fn defer(&mut self, tid: TxId, mut candidate: Candidate, due: rt::Instant) {
        candidate.state = CandidateState::Deferred;
        candidate.reported_again = false;
        candidate.due = due;
        self.counts[usize::from(candidate.from_scan)] += 1;
        self.deferred.insert((due, tid.clone()));
        self.entries.insert(tid, candidate);
    }

    fn ready_len(&self) -> usize {
        self.ready.iter().map(BTreeSet::len).sum()
    }

    fn oldest_ready(&self) -> Option<rt::Instant> {
        self.ready
            .iter()
            .filter_map(|queue| queue.first().map(|(due, _)| *due))
            .min()
    }

    fn next_due(&self) -> Option<rt::Instant> {
        self.deferred.first().map(|(due, _)| *due)
    }

    fn record_backlog(&self, counters: &Counters) {
        counters
            .ready
            .store(self.ready_len() as u64, Ordering::Relaxed);
        counters
            .waiting
            .store(self.deferred.len() as u64, Ordering::Relaxed);
        counters
            .in_flight
            .store(self.running.iter().sum::<usize>() as u64, Ordering::Relaxed);
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
    mon: Monitor,
    timeline: Timeline,
    hints: GcHints,
    retry_delay: Duration,
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
        mon: Monitor,
        hints: GcHints,
        retry_delay: Duration,
    ) -> Self {
        Self {
            tl,
            structural_intents,
            collection_lifecycle,
            router: TreeRouter::new(nodes, std::num::NonZeroUsize::MIN),
            locker,
            mon,
            timeline,
            hints,
            retry_delay,
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
        let tl = self.tl.clone();
        let hints = self.hints.clone();
        let retry_delay = self.retry_delay;
        let gc = self;
        let counters = hints.counters.clone();
        let mut scan = Some(GcScan::new(tl, counters.clone(), retry_delay));
        let mut cadence = ScanCadence::new(
            (retry_delay / 16).max(Duration::from_millis(1)),
            retry_delay * 40,
        );
        let mut next_scan = rt::Instant::now() + retry_delay;
        let mut candidates = Candidates::default();
        let mut scanned = VecDeque::new();
        let mut checks = FuturesUnordered::<Check>::new();
        let mut listing = FuturesUnordered::<Listing>::new();
        let mut concurrency = 1;
        let mut scan_useful = false;
        let mut scan_failed = false;
        loop {
            let now = rt::Instant::now();
            let (reports, more_hints) = hints.drain();
            for tid in reports {
                candidates.admit(tid, false, now, &counters);
            }
            for _ in 0..ADMISSION_BATCH {
                let Some(tid) = scanned.pop_front() else {
                    break;
                };
                candidates.admit(tid, true, now, &counters);
            }
            candidates.promote_due(now);
            let ready = candidates.ready_len();
            let oldest = candidates
                .oldest_ready()
                .map(|due| now.saturating_duration_since(due))
                .unwrap_or_default();
            if ready > concurrency || oldest >= retry_delay / 16 {
                concurrency = (concurrency * 2).min(MAX_CHECKS);
            } else if ready == 0 && checks.is_empty() {
                concurrency = 1;
            }
            while checks.len() < concurrency {
                let Some(tid) = candidates.take_ready() else {
                    break;
                };
                let gc = gc.clone();
                checks.push(
                    async move {
                        let result = gc.check_candidate(&tid).await;
                        (tid, result)
                    }
                    .boxed(),
                );
            }
            let scan_count = candidates.counts[1];
            let can_list =
                scan.is_some() && scanned.is_empty() && scan_count + PAGE_SIZE <= SCAN_CAPACITY;
            if can_list && now >= next_scan {
                let mut current = scan.take().unwrap();
                listing.push(
                    async move {
                        let result = current.turn().await;
                        (current, result)
                    }
                    .boxed(),
                );
            }
            candidates.record_backlog(&counters);
            let next_due = candidates.next_due();
            let local_work =
                more_hints || !scanned.is_empty() || next_due.is_some_and(|due| due <= now);
            let deadline = next_due
                .into_iter()
                .chain(can_list.then_some(next_scan))
                .min()
                .unwrap_or(now + retry_delay * 40)
                .max(now + Duration::from_millis(1));
            tokio::select! {
                biased;
                Some((current, result)) = listing.next(), if !listing.is_empty() => {
                    let continued = current.has_continuation();
                    let delay = match result {
                        Ok(Some(ids)) => {
                            if !scan_failed { cadence.observe(scan_useful); }
                            scan_useful = false;
                            scan_failed = false;
                            scanned.extend(ids);
                            let delay = cadence.delay();
                            if continued { delay.min(retry_delay) } else { delay }
                        }
                        Ok(None) => current.retry_at().map(|at| at.saturating_duration_since(rt::Instant::now())).unwrap_or(retry_delay),
                        Err(error) => {
                            tracing::debug!(error = %error, "GC scan deferred");
                            counters.failures.fetch_add(1, Ordering::Relaxed);
                            retry_delay
                        }
                    };
                    next_scan = rt::Instant::now() + delay;
                    scan = Some(current);
                }
                Some((tid, result)) = checks.next(), if !checks.is_empty() => {
                    let mut completed = Some((tid, result));
                    for _ in 0..MAX_CHECKS {
                        let Some((tid, result)) = completed.take() else { break; };
                        let mut candidate = candidates.complete(&tid);
                        let retry = match result {
                            Ok(GcOutcome::Reclaimed) => {
                                counters.progress.fetch_add(1, Ordering::Relaxed);
                                scan_useful |= candidate.from_scan;
                                None
                            }
                            Ok(GcOutcome::Progress) => {
                                counters.progress.fetch_add(1, Ordering::Relaxed);
                                scan_useful |= candidate.from_scan;
                                Some(retry_delay)
                            }
                            Ok(GcOutcome::Deferred(delay)) => Some(delay),
                            Ok(GcOutcome::Retained) => candidate.reported_again.then_some(retry_delay),
                            Err(error) => {
                                tracing::debug!(tx = %tid, error = %error, "GC candidate check deferred");
                                counters.failures.fetch_add(1, Ordering::Relaxed);
                                scan_failed |= candidate.from_scan;
                                candidate.failures = candidate.failures.saturating_add(1);
                                Some(retry_delay * (1u32 << candidate.failures.min(4)))
                            }
                        };
                        if let Some(delay) = retry {
                            candidates.defer(tid, candidate, rt::Instant::now() + delay);
                        }
                        completed = checks.next().now_or_never().flatten();
                    }
                }
                _ = std::future::ready(()), if local_work => {}
                _ = hints.wake.notified() => {}
                _ = rt::sleep(deadline.saturating_duration_since(rt::Instant::now())) => {}
            }
            // GC admission must yield to transaction tasks even on an in-memory backend.
            rt::yield_now().await;
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
