//! Pipelined GC checks that share currentness barriers (ADR-078, ADR-079).

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use glassdb_concurr::rt;
use glassdb_data::TxId;
use glassdb_storage::{CurrentnessBarrier, Observation, transaction::TxRecord};

use super::{Gc, GcEligibility, GcOutcome, PAGE_SIZE};
use crate::error::TransError;

// Records that wait for an epoch hold only their observed record, not a check
// slot. Leaf reads are shared only when an epoch holds many records compared to
// the number of leaves they touch, so an epoch can be several LIST pages.
const MAX_EPOCH: usize = 8 * PAGE_SIZE;
// A reclaimable candidate waits at most this long for others to join its
// barrier. This is short compared to the retention period. Tests of GC
// scheduling advance the clock past it.
pub(super) const EPOCH_WAIT: Duration = Duration::from_secs(1);

type StatusRead = BoxFuture<'static, (TxId, Result<GcEligibility, TransError>)>;
type ReferenceCheck = BoxFuture<'static, (TxId, Result<GcOutcome, TransError>)>;
type Reclaimable = (TxId, Observation<TxRecord>);

/// Runs GC checks with at most `limit` backend operations in flight.
///
/// A check first reads its candidate's status. A candidate that GC can reclaim
/// then waits for an epoch: one currentness barrier for every candidate whose
/// status read completed before it. The reference checks of an epoch share
/// their leaf reads, and each one completes independently of the others.
///
/// The pipeline assumes that more candidates follow, so a forming epoch waits
/// for them until it is full or [`EPOCH_WAIT`] ends. A caller that has no more
/// candidates for now calls [`CheckPipeline::flush`]. While status reads stay
/// in flight, as under continuous GC work, an epoch waits the full
/// [`EPOCH_WAIT`], which is short compared to the retention period.
pub(super) struct CheckPipeline {
    gc: Gc,
    limit: NonZeroUsize,
    queued: VecDeque<TxId>,
    flushed: bool,
    status_reads: FuturesUnordered<StatusRead>,
    forming: Vec<Reclaimable>,
    forming_since: Option<rt::Instant>,
    sealed: Option<(CurrentnessBarrier, VecDeque<Reclaimable>)>,
    reference_checks: FuturesUnordered<ReferenceCheck>,
}

impl CheckPipeline {
    pub(super) fn new(gc: Gc, limit: NonZeroUsize) -> Self {
        Self {
            gc,
            limit,
            queued: VecDeque::new(),
            flushed: false,
            status_reads: FuturesUnordered::new(),
            forming: Vec::new(),
            forming_since: None,
            sealed: None,
            reference_checks: FuturesUnordered::new(),
        }
    }

    pub(super) fn set_limit(&mut self, limit: NonZeroUsize) {
        self.limit = limit;
    }

    /// Queues the check of `tid`. It cancels an earlier flush.
    pub(super) fn push(&mut self, tid: TxId) {
        self.queued.push_back(tid);
        self.flushed = false;
    }

    /// Tells the pipeline that no more candidates come for now, so that the
    /// forming epoch can seal as soon as its status reads complete.
    pub(super) fn flush(&mut self) {
        self.flushed = true;
    }

    /// Reports whether no check is queued, running, or waiting for an epoch.
    pub(super) fn is_idle(&self) -> bool {
        self.queued.is_empty()
            && self.status_reads.is_empty()
            && self.forming.is_empty()
            && self.sealed.is_none()
            && self.reference_checks.is_empty()
    }

    /// Returns the number of candidates whose check has not started.
    pub(super) fn queued(&self) -> usize {
        self.queued.len()
    }

    /// Reports whether the pipeline wants another candidate. It keeps about one
    /// round of queued candidates, so that it can refill a slot as soon as a
    /// check completes. A caller that chooses among its candidates then keeps
    /// the choice for every later one.
    pub(super) fn wants_candidates(&self) -> bool {
        self.queued.len() < self.limit.get()
    }

    /// Returns the number of backend checks in flight.
    pub(super) fn in_flight(&self) -> usize {
        self.status_reads.len() + self.reference_checks.len()
    }

    /// Returns the number of candidates that wait for a slot or an epoch.
    pub(super) fn waiting(&self) -> usize {
        self.queued.len()
            + self.forming.len()
            + self.sealed.as_ref().map_or(0, |(_, sealed)| sealed.len())
    }

    /// Returns the next completed check, or `None` when no check is left.
    ///
    /// Cancel-safe: dropping the future loses no completed status read or
    /// check.
    pub(super) async fn next(&mut self) -> Option<(TxId, Result<GcOutcome, TransError>)> {
        loop {
            self.refill();
            if self.is_idle() {
                return None;
            }
            let wait = self
                .forming_since
                .filter(|_| self.sealed.is_none())
                .map(|since| (since + EPOCH_WAIT).saturating_duration_since(rt::Instant::now()));
            tokio::select! {
                biased;
                Some((tid, result)) = self.reference_checks.next(), if !self.reference_checks.is_empty() => {
                    self.refill();
                    return Some((tid, result));
                }
                Some((tid, result)) = self.status_reads.next(), if !self.status_reads.is_empty() => {
                    let outcome = match result {
                        Ok(GcEligibility::Ready(observation)) => {
                            self.forming_since.get_or_insert_with(rt::Instant::now);
                            self.forming.push((tid, observation));
                            continue;
                        }
                        Ok(GcEligibility::Deferred(delay)) => Ok(GcOutcome::Deferred(delay)),
                        Ok(GcEligibility::Retained) => Ok(GcOutcome::Retained),
                        Ok(GcEligibility::Missing) => Ok(GcOutcome::Missing),
                        Err(error) => Err(error),
                    };
                    self.refill();
                    return Some((tid, outcome));
                }
                _ = rt::sleep(wait.unwrap_or_default()), if wait.is_some() => {}
            }
        }
    }

    /// Gives every free slot to waiting work: first to the reference checks of
    /// a sealed epoch, then to the status reads of queued candidates.
    fn refill(&mut self) {
        self.seal_if_due();
        self.start_reference_checks();
        // The sealed epoch can drain above, which lets a due epoch seal before
        // new status reads take the slots.
        self.seal_if_due();
        self.start_reference_checks();
        self.start_status_reads();
    }

    /// Reports whether more candidates can still join the forming epoch.
    fn epoch_can_grow(&self) -> bool {
        let joining = self.forming.len() + self.status_reads.len();
        joining < MAX_EPOCH
            && (!self.status_reads.is_empty() || !self.queued.is_empty() || !self.flushed)
    }

    /// Captures the barrier of the forming epoch when no earlier epoch waits
    /// for a slot, and the epoch is full, has waited long enough, or cannot
    /// grow.
    fn seal_if_due(&mut self) {
        let Some(since) = self.forming_since else {
            return;
        };
        if self.sealed.is_some() || (self.epoch_can_grow() && since.elapsed() < EPOCH_WAIT) {
            return;
        }
        // Every status read of the epoch completed before this barrier, so
        // leaf reads after it see each reference of these records (ADR-079).
        let barrier = self.gc.timeline.currentness_barrier();
        self.sealed = Some((barrier, self.forming.drain(..).collect()));
        self.forming_since = None;
    }

    /// Gives free slots to the reference checks of the sealed epoch first, so
    /// that an epoch drains before the next one grows.
    fn start_reference_checks(&mut self) {
        let Some((barrier, sealed)) = &mut self.sealed else {
            return;
        };
        let barrier = *barrier;
        while self.status_reads.len() + self.reference_checks.len() < self.limit.get()
            && let Some((tid, observation)) = sealed.pop_front()
        {
            let gc = self.gc.clone();
            self.reference_checks.push(
                async move {
                    // Yield so concurrent writers can publish observations
                    // that satisfy the barrier; otherwise GC adds backend reads
                    // for leaves those writers are already refreshing.
                    rt::yield_now().await;
                    let result = gc.try_reclaim(&tid, &observation, barrier).await;
                    (tid, result)
                }
                .boxed(),
            );
        }
        if sealed.is_empty() {
            self.sealed = None;
        }
    }

    fn start_status_reads(&mut self) {
        while self.sealed.is_none()
            && self.in_flight() < self.limit.get()
            && self.forming.len() + self.status_reads.len() < MAX_EPOCH
            && let Some(tid) = self.queued.pop_front()
        {
            let gc = self.gc.clone();
            self.status_reads.push(
                async move {
                    // The status read needs evidence from after its own start, so
                    // that an earlier cached status cannot authorize deletion.
                    let barrier = gc.timeline.currentness_barrier();
                    let result = gc.filter_candidate(&tid, barrier).await;
                    (tid, result)
                }
                .boxed(),
            );
        }
    }
}
