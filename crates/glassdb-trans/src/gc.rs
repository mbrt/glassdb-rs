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

type Check = BoxFuture<'static, (TxId, Result<GcOutcome, TransError>)>;
type Listing = BoxFuture<'static, (GcScan, Result<Option<Vec<TxId>>, StorageError>)>;

struct Candidate {
    due: rt::Instant,
    from_scan: bool,
    running: bool,
    reported_again: bool,
    failures: u32,
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
        let mut candidates = BTreeMap::<TxId, Candidate>::new();
        let mut checks = FuturesUnordered::<Check>::new();
        let mut listing = FuturesUnordered::<Listing>::new();
        let mut concurrency = 1;
        let mut scan_useful = false;
        let mut scan_failed = false;
        loop {
            let now = rt::Instant::now();
            for tid in hints.drain() {
                Self::admit(&mut candidates, tid, false, now, &counters);
            }
            let ready = candidates
                .values()
                .filter(|c| !c.running && c.due <= now)
                .count();
            let oldest = candidates
                .values()
                .filter(|c| !c.running && c.due <= now)
                .map(|c| now.duration_since(c.due))
                .max()
                .unwrap_or_default();
            if ready > concurrency || oldest >= retry_delay / 16 {
                concurrency = (concurrency * 2).min(MAX_CHECKS);
            } else if ready == 0 && checks.is_empty() {
                concurrency = 1;
            }
            while checks.len() < concurrency {
                let scan_running = candidates.values().any(|c| c.running && c.from_scan);
                let selected = candidates
                    .iter()
                    .filter(|(_, c)| !c.running && c.due <= now)
                    .min_by_key(|(_, c)| (scan_running || !c.from_scan, c.due))
                    .map(|(tid, _)| tid.clone());
                let Some(tid) = selected else {
                    break;
                };
                candidates.get_mut(&tid).unwrap().running = true;
                let gc = gc.clone();
                checks.push(
                    async move {
                        let result = gc.check_candidate(&tid).await;
                        (tid, result)
                    }
                    .boxed(),
                );
            }
            let scan_count = candidates.values().filter(|c| c.from_scan).count();
            let can_list = scan.is_some() && scan_count + PAGE_SIZE <= SCAN_CAPACITY;
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
            Self::record_backlog(&candidates, now, &counters);
            let next_due = candidates
                .values()
                .filter(|c| !c.running && c.due > now)
                .map(|c| c.due)
                .min();
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
                            for tid in ids {
                                Self::admit(&mut candidates, tid, true, rt::Instant::now(), &counters);
                            }
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
                    let mut candidate = candidates.remove(&tid).unwrap();
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
                        candidate.running = false;
                        candidate.reported_again = false;
                        candidate.due = rt::Instant::now() + delay;
                        candidates.insert(tid, candidate);
                    }
                }
                _ = hints.wake.notified() => {}
                _ = rt::sleep(deadline.saturating_duration_since(rt::Instant::now())) => {}
            }
            // GC admission must yield to transaction tasks even on an in-memory backend.
            rt::yield_now().await;
        }
    }

    fn admit(
        candidates: &mut BTreeMap<TxId, Candidate>,
        tid: TxId,
        from_scan: bool,
        now: rt::Instant,
        counters: &Arc<Counters>,
    ) {
        if let Some(candidate) = candidates.get_mut(&tid) {
            candidate.from_scan |= from_scan;
            candidate.reported_again |= candidate.running;
            return;
        }
        let count = candidates
            .values()
            .filter(|c| c.from_scan == from_scan)
            .count();
        let capacity = if from_scan {
            SCAN_CAPACITY
        } else {
            HINT_CAPACITY
        };
        if count >= capacity {
            counters.dropped_candidates.fetch_add(1, Ordering::Relaxed);
            return;
        }
        candidates.insert(
            tid,
            Candidate {
                due: now,
                from_scan,
                running: false,
                reported_again: false,
                failures: 0,
            },
        );
    }

    fn record_backlog(
        candidates: &BTreeMap<TxId, Candidate>,
        now: rt::Instant,
        counters: &Counters,
    ) {
        let mut ready = 0;
        let mut deferred = 0;
        let mut running = 0;
        let mut oldest: Option<rt::Instant> = None;
        for candidate in candidates.values() {
            if candidate.running {
                running += 1;
            } else if candidate.due > now {
                deferred += 1;
            } else {
                ready += 1;
                oldest = Some(oldest.map_or(candidate.due, |due| due.min(candidate.due)));
            }
        }
        counters.ready.store(ready, Ordering::Relaxed);
        counters.waiting.store(deferred, Ordering::Relaxed);
        counters.in_flight.store(running, Ordering::Relaxed);
        *counters.oldest_ready.lock().unwrap() = oldest;
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
            queue.push_back(tid);
            drop(queue);
            self.wake.notify_one();
        }
    }

    #[cfg(test)]
    pub(crate) fn pending(&self) -> Vec<TxId> {
        self.queue.lock().unwrap().iter().cloned().collect()
    }

    fn drain(&self) -> Vec<TxId> {
        let queued = std::mem::take(&mut *self.queue.lock().unwrap());
        let mut seen = BTreeSet::new();
        queued
            .into_iter()
            .filter(|tid| seen.insert(tid.clone()))
            .collect()
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
