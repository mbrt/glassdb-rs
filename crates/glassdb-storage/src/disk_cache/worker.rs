//! Persistent-cache work queues, lifecycle, and worker policy.

use std::io;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

use super::admission::{Admission, OptionalReservation, PayloadReservation, PromotionReservation};
use super::disk::{Disk, WriterState, open_disk};
use super::fence::{FenceContext, FenceGuard, FenceTracker, PathFence};
use super::format::CacheGeometry;
use super::media::CacheMedia;
use super::{EncodedBody, OpenedPersistentCache, PersistentCache, PersistentCacheConfig};

const WORK_QUEUE_ITEMS: usize = 4096;
const SYNC_INTERVAL: Duration = Duration::from_secs(5);
// One token per seven admission bytes bounds promotions to one eighth of
// combined append traffic; the segment cap also limits short bursts.
const PROMOTION_EARN_DIVISOR: u64 = 7;
const PROMOTION_CAP_DIVISOR: u64 = 8;

impl PersistentCache {
    pub(super) fn attach_worker(
        opened: OpenedPersistentCache,
        worker: rt::DedicatedJoinHandle<()>,
    ) -> OpenedPersistentCache {
        if let Some(inner) = &opened.cache.inner {
            inner.attach_worker(worker);
        }
        opened
    }

    pub(super) fn spawn_worker(
        config: PersistentCacheConfig,
        database_name: String,
        database_id: DatabaseId,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
        media: Arc<dyn CacheMedia>,
        completion: oneshot::Sender<OpenedPersistentCache>,
    ) -> Result<rt::DedicatedJoinHandle<()>, rt::SpawnError> {
        rt::spawn_dedicated("glassdb-l2", async move {
            Self::run_opening_worker(
                config,
                database_name,
                database_id,
                geometry,
                metrics,
                media,
                completion,
            )
            .await;
        })
    }

    async fn run_opening_worker(
        config: PersistentCacheConfig,
        database_name: String,
        database_id: DatabaseId,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
        media: Arc<dyn CacheMedia>,
        completion: oneshot::Sender<OpenedPersistentCache>,
    ) {
        let (disk, writer, last_sequence_point) = match open_disk(
            config,
            &database_name,
            database_id,
            geometry,
            metrics.clone(),
            media,
        )
        .await
        {
            Ok(opened) => opened,
            Err(error) => {
                metrics.l2_error();
                tracing::warn!(error = %error, "persistent cache disabled during initialization");
                let _ = completion.send(Self::disabled_open(metrics));
                return;
            }
        };
        let (inner, worker) = CacheInner::prepare(disk, writer, metrics.clone());
        let opened = OpenedPersistentCache {
            cache: Self {
                inner: Some(Arc::new(inner)),
                metrics,
            },
            last_sequence_point,
        };
        // A timed-out opener drops the receiver, so the worker must release the
        // file lock instead of becoming an unreachable detached cache.
        if completion.send(opened).is_ok() {
            worker.run().await;
        }
    }
}

pub(super) struct CacheInner {
    pub(super) shared: Arc<Shared>,
    sender: mpsc::Sender<Work>,
    // Orders producers against shutdown so no work enters after shutdown starts.
    enqueue_gate: Mutex<()>,
    shutdown_started: AtomicBool,
    pub(super) completion: Arc<Completion>,
    worker_joined: Completion,
    worker: Mutex<Option<rt::DedicatedJoinHandle<()>>>,
}

impl CacheInner {
    pub(super) fn abort_worker(&self) {
        if let Some(worker) = self.worker.lock().unwrap().as_ref() {
            worker.abort();
        }
    }

    pub(super) async fn shutdown(&self) {
        if self
            .shutdown_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.shared
                .shutdown_requested
                .store(true, Ordering::Release);
            // A full queue already wakes the worker; the sentinel is only
            // needed to wake an idle receiver.
            self.wake_for_shutdown();
        }
        self.completion.wait().await;
        self.join_worker().await;
    }

    pub(super) fn enqueue_required(&self, work: Work) {
        let _gate = self.enqueue_gate.lock().unwrap();
        if self.shared.shutdown_requested.load(Ordering::Acquire)
            || !self.shared.enabled.load(Ordering::Acquire)
        {
            return;
        }
        match self.sender.try_send(work) {
            Ok(()) => {}
            Err(TrySendError::Full(work)) => {
                self.disable_message("persistent-cache required-work queue is full");
                drop(work);
            }
            Err(TrySendError::Closed(work)) => {
                self.disable_message("persistent-cache worker stopped");
                drop(work);
            }
        }
    }

    pub(super) fn enqueue_optional(
        &self,
        build_work: impl FnOnce(OptionalReservation) -> Work,
    ) -> bool {
        let _gate = self.enqueue_gate.lock().unwrap();
        if self.shared.shutdown_requested.load(Ordering::Acquire)
            || !self.shared.enabled.load(Ordering::Acquire)
        {
            return false;
        }
        let Some(optional) = self.shared.admission.reserve_optional() else {
            return false;
        };
        match self.sender.try_send(build_work(optional)) {
            Ok(()) => true,
            Err(TrySendError::Full(_) | TrySendError::Closed(_)) => false,
        }
    }

    pub(super) fn disable_message(&self, message: &'static str) {
        if self.shared.disable() {
            tracing::warn!("{message}");
        }
    }

    fn prepare(
        disk: Arc<Disk>,
        writer: WriterState,
        metrics: Arc<CacheMetrics>,
    ) -> (Self, CacheWorker) {
        let (sender, receiver) = mpsc::channel(WORK_QUEUE_ITEMS);
        let completion = Arc::new(Completion::new());
        let shared = Arc::new(Shared {
            disk,
            enabled: AtomicBool::new(true),
            metrics,
            admission: Arc::new(Admission::new(writer.filter.clone())),
            fences: FenceTracker::new(),
            shutdown_requested: AtomicBool::new(false),
        });
        (
            Self {
                shared: shared.clone(),
                sender,
                enqueue_gate: Mutex::new(()),
                shutdown_started: AtomicBool::new(false),
                completion: completion.clone(),
                worker_joined: Completion::new(),
                worker: Mutex::new(None),
            },
            CacheWorker {
                shared,
                writer,
                receiver,
                completion,
            },
        )
    }

    fn attach_worker(&self, worker: rt::DedicatedJoinHandle<()>) {
        *self.worker.lock().unwrap() = Some(worker);
    }

    async fn join_worker(&self) {
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.await;
            self.worker_joined.finish();
        } else {
            self.worker_joined.wait().await;
        }
    }

    fn wake_for_shutdown(&self) {
        let _gate = self.enqueue_gate.lock().unwrap();
        let _ = self.sender.try_send(Work::Shutdown);
    }
}

impl Drop for CacheInner {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.get_mut().unwrap().take() {
            worker.abort();
        }
    }
}

struct CacheWorker {
    shared: Arc<Shared>,
    writer: WriterState,
    receiver: mpsc::Receiver<Work>,
    completion: Arc<Completion>,
}

impl CacheWorker {
    async fn run(self) {
        let _completion = CompletionGuard(self.completion);
        run_worker(self.shared, self.writer, self.receiver).await;
    }
}

pub(super) struct Shared {
    pub(super) disk: Arc<Disk>,
    pub(super) enabled: AtomicBool,
    metrics: Arc<CacheMetrics>,
    pub(super) admission: Arc<Admission>,
    pub(super) fences: FenceTracker,
    shutdown_requested: AtomicBool,
}

impl Shared {
    pub(super) fn disable(&self) -> bool {
        if self.enabled.swap(false, Ordering::AcqRel) {
            self.metrics.l2_error();
            true
        } else {
            false
        }
    }
}

pub(super) struct Completion {
    done: AtomicBool,
    notify: Notify,
}

impl Completion {
    pub(super) async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

struct CompletionGuard(Arc<Completion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

pub(super) enum Work {
    Lookup {
        path: Arc<str>,
        completion: oneshot::Sender<Option<EncodedBody>>,
        optional: OptionalReservation,
    },
    Replace {
        path: Arc<str>,
        revision: Vec<u8>,
        body: Vec<u8>,
        current_after: SequencePoint,
        // Drop releases the path only after publication finishes.
        fence: FenceGuard,
        payload: PayloadReservation,
    },
    Invalidate {
        path: Arc<str>,
        // Drop releases the path only after invalidation finishes.
        fence: FenceGuard,
    },
    Promote {
        context: Arc<dyn FenceContext>,
        epoch: u64,
        optional: OptionalReservation,
        promotion: PromotionReservation,
    },
    Shutdown,
}

async fn lookup(shared: &Shared, path: &str) -> Option<EncodedBody> {
    if !shared.enabled.load(Ordering::Acquire) {
        return None;
    }
    match shared.disk.lookup(path).await {
        Ok(Some(record)) => {
            shared.metrics.l2_hit();
            Some(EncodedBody {
                revision: record.revision,
                body: record.body,
                current_after: record.current_after,
            })
        }
        Ok(None) => {
            shared.metrics.l2_miss();
            None
        }
        Err(error) => {
            if shared.disable() {
                tracing::warn!(%error, "persistent-cache lookup failed");
            }
            None
        }
    }
}

async fn run_worker(
    shared: Arc<Shared>,
    mut writer: WriterState,
    mut receiver: mpsc::Receiver<Work>,
) {
    let mut last_sync = rt::Instant::now();
    loop {
        let work = if shared.shutdown_requested.load(Ordering::Acquire) {
            match receiver.try_recv() {
                Ok(work) => work,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => Work::Shutdown,
            }
        } else {
            tokio::select! {
                biased;
                work = receiver.recv() => {
                    let Some(work) = work else {
                        break;
                    };
                    work
                }
                _ = rt::sleep(SYNC_INTERVAL) => {
                    sync_writer_if_needed(&shared, &mut writer, &mut last_sync, true).await;
                    continue;
                }
            }
        };
        if handle_work(&shared, &mut writer, work).await.is_break() {
            break;
        }
        sync_writer_if_needed(&shared, &mut writer, &mut last_sync, false).await;
    }
}

async fn handle_work(shared: &Shared, writer: &mut WriterState, work: Work) -> ControlFlow<()> {
    match work {
        Work::Lookup {
            path,
            completion,
            optional,
        } => handle_lookup(shared, path, completion, optional).await,
        Work::Replace {
            path,
            revision,
            body,
            current_after,
            fence,
            payload,
        } => {
            handle_replace(
                shared,
                writer,
                path,
                revision,
                body,
                current_after,
                fence,
                payload,
            )
            .await
        }
        Work::Invalidate { path, fence } => handle_invalidate(shared, writer, path, fence).await,
        Work::Promote {
            context,
            epoch,
            optional,
            promotion,
        } => handle_promote(shared, writer, context, epoch, optional, promotion).await,
        Work::Shutdown => handle_shutdown(shared, writer).await,
    }
}

async fn handle_lookup(
    shared: &Shared,
    path: Arc<str>,
    completion: oneshot::Sender<Option<EncodedBody>>,
    optional: OptionalReservation,
) -> ControlFlow<()> {
    // Queue pressure excludes work once its handler starts running.
    drop(optional);
    let _ = completion.send(lookup(shared, &path).await);
    ControlFlow::Continue(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_replace(
    shared: &Shared,
    writer: &mut WriterState,
    path: Arc<str>,
    revision: Vec<u8>,
    body: Vec<u8>,
    current_after: SequencePoint,
    fence: FenceGuard,
    payload: PayloadReservation,
) -> ControlFlow<()> {
    // Payload pressure bounds queued bytes, not the worker's active append.
    drop(payload);
    let result = if shared.enabled.load(Ordering::Acquire) && fence.is_current() {
        let result = writer.append(&path, &revision, &body, current_after).await;
        if let Ok(slot) = result {
            let earned = slot.record_bytes / PROMOTION_EARN_DIVISOR;
            let cap = writer.disk.format.maximum_record_bytes() / PROMOTION_CAP_DIVISOR;
            writer.promotion_tokens = writer.promotion_tokens.saturating_add(earned).min(cap);
        }
        result.map(|_| ())
    } else {
        Ok(())
    };
    disable_after_work_error(shared, &result);
    drop(fence);
    ControlFlow::Continue(())
}

async fn handle_invalidate(
    shared: &Shared,
    writer: &mut WriterState,
    path: Arc<str>,
    fence: FenceGuard,
) -> ControlFlow<()> {
    let result = if shared.enabled.load(Ordering::Acquire) {
        writer.invalidate(&path).await
    } else {
        Ok(())
    };
    disable_after_work_error(shared, &result);
    drop(fence);
    ControlFlow::Continue(())
}

async fn handle_promote(
    shared: &Shared,
    writer: &mut WriterState,
    context: Arc<dyn FenceContext>,
    epoch: u64,
    optional: OptionalReservation,
    promotion: PromotionReservation,
) -> ControlFlow<()> {
    // Queue pressure excludes work once its handler starts running.
    drop(optional);
    let result = if shared.enabled.load(Ordering::Acquire) {
        promote(shared, writer, promotion.path(), context.fence(), epoch).await
    } else {
        Ok(())
    };
    disable_after_work_error(shared, &result);
    drop(promotion);
    ControlFlow::Continue(())
}

async fn handle_shutdown(shared: &Shared, writer: &mut WriterState) -> ControlFlow<()> {
    clean_shutdown(shared, writer).await;
    ControlFlow::Break(())
}

async fn sync_writer_if_needed(
    shared: &Shared,
    writer: &mut WriterState,
    last_sync: &mut rt::Instant,
    force: bool,
) {
    if !shared.enabled.load(Ordering::Acquire) {
        return;
    }
    let force = force || last_sync.elapsed() >= SYNC_INTERVAL;
    let result = if force {
        writer.sync().await
    } else {
        writer.sync_if_needed().await
    };
    match result {
        Ok(true) => *last_sync = rt::Instant::now(),
        Ok(false) => {}
        Err(error) if shared.disable() => {
            tracing::warn!(%error, "persistent cache disabled after sync failure");
        }
        Err(_) => {}
    }
}

async fn clean_shutdown(shared: &Shared, writer: &mut WriterState) {
    if shared.enabled.load(Ordering::Acquire)
        && let Err(error) = writer.clean_shutdown().await
        && shared.disable()
    {
        tracing::warn!(%error, "persistent cache clean shutdown failed");
    }
}

fn disable_after_work_error(shared: &Shared, result: &io::Result<()>) {
    if let Err(error) = result
        && shared.disable()
    {
        tracing::warn!(%error, "persistent cache worker disabled after I/O failure");
    }
}

async fn promote(
    shared: &Shared,
    writer: &mut WriterState,
    path: &str,
    fence: &PathFence,
    epoch: u64,
) -> io::Result<()> {
    if fence.snapshot() != (epoch, false) {
        return Ok(());
    }
    let Some(slot) = shared.disk.current_slot(path).await? else {
        return Ok(());
    };
    let mut generations = shared
        .disk
        .segment_generations
        .iter()
        .map(|generation| generation.load(Ordering::Acquire))
        .filter(|generation| *generation != 0)
        .collect::<Vec<_>>();
    generations.sort_unstable();
    if generations.len() < 2
        || generations
            .iter()
            .position(|generation| *generation == slot.generation)
            .is_none_or(|rank| rank >= generations.len() / 2)
    {
        return Ok(());
    }
    if writer.promotion_tokens < slot.record_bytes {
        return Ok(());
    }
    let Some(record) = shared.disk.read_record(path, slot).await? else {
        return Ok(());
    };
    if fence.snapshot() != (epoch, false) || shared.disk.current_slot(path).await? != Some(slot) {
        return Ok(());
    }
    let promoted = writer
        .append(path, &record.revision, &record.body, record.current_after)
        .await?;
    writer.promotion_tokens -= promoted.record_bytes;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::super::format::COMPACT_GEOMETRY;
    use super::super::sim_media::{MediaFaultProfile, SimMedia};
    use super::*;

    const TEST_CAPACITY: u64 = 2 * 1024 * 1024;

    struct Fixture {
        _directory: TempDir,
        media: SimMedia,
        inner: CacheInner,
        worker: Option<CacheWorker>,
    }

    async fn fixture() -> Fixture {
        let directory = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let metrics = Arc::new(CacheMetrics::new());
        let config = PersistentCacheConfig {
            directory: directory.path().to_path_buf(),
            capacity_bytes: TEST_CAPACITY,
        };
        let (disk, writer, _) = open_disk(
            config,
            "db",
            DatabaseId::from_bytes([1; 16]),
            COMPACT_GEOMETRY,
            metrics.clone(),
            Arc::new(media.clone()),
        )
        .await
        .unwrap();
        let (inner, worker) = CacheInner::prepare(disk, writer, metrics);
        Fixture {
            _directory: directory,
            media,
            inner,
            worker: Some(worker),
        }
    }

    fn point(value: u64) -> SequencePoint {
        SequencePoint::from_raw(value)
    }

    fn assert_no_reservations(shared: &Shared) {
        assert_eq!(shared.admission.reservation_counts(), (0, 0, 0));
        assert_eq!(shared.fences.active_count(), 0);
    }

    async fn fail_work(fixture: &mut Fixture, work: Work) -> ControlFlow<()> {
        fixture.media.make_permanently_unavailable();
        let shared = fixture.inner.shared.clone();
        let CacheWorker { mut writer, .. } = fixture.worker.take().unwrap();
        let flow = handle_work(&shared, &mut writer, work).await;
        assert_no_reservations(&shared);
        flow
    }

    async fn cancel_work(
        fixture: &mut Fixture,
        work: Work,
        in_flight_admission: (u64, usize, usize),
        in_flight_fences: usize,
    ) {
        let mut pause = fixture.media.pause_next_operation();
        let shared = fixture.inner.shared.clone();
        let task_shared = shared.clone();
        let CacheWorker { mut writer, .. } = fixture.worker.take().unwrap();
        let task = tokio::spawn(async move { handle_work(&task_shared, &mut writer, work).await });
        pause.wait_until_entered().await;
        assert_eq!(shared.admission.reservation_counts(), in_flight_admission);
        assert_eq!(shared.fences.active_count(), in_flight_fences);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        pause.resume();
        assert_no_reservations(&shared);
    }

    #[tokio::test]
    async fn rejected_enqueues_release_all_work_reservations() {
        let mut fixture = fixture().await;
        drop(fixture.worker.take());
        let shared = fixture.inner.shared.clone();

        let path = Arc::<str>::from("db/promote");
        let promotion = shared.admission.reserve_promotion(&path).unwrap();
        let context = Arc::new(PathFence::default());
        let (epoch, active) = context.snapshot();
        assert!(!active);
        assert!(
            !fixture
                .inner
                .enqueue_optional(move |optional| Work::Promote {
                    context,
                    epoch,
                    optional,
                    promotion,
                })
        );
        assert_no_reservations(&shared);

        let context = Arc::new(PathFence::default());
        let fence = shared.fences.begin(context.clone()).unwrap();
        let payload = shared.admission.reserve_payload(123).unwrap();
        fixture.inner.enqueue_required(Work::Replace {
            path: Arc::from("db/replace"),
            revision: b"r1".to_vec(),
            body: b"body".to_vec(),
            current_after: point(1),
            fence,
            payload,
        });
        assert_no_reservations(&shared);
        assert!(!context.is_active());
    }

    #[tokio::test]
    async fn handler_errors_release_all_work_reservations() {
        let mut lookup = fixture().await;
        let shared = lookup.inner.shared.clone();
        let optional = shared.admission.reserve_optional().unwrap();
        let (completion, result) = oneshot::channel();
        assert_eq!(
            fail_work(
                &mut lookup,
                Work::Lookup {
                    path: Arc::from("db/lookup"),
                    completion,
                    optional,
                },
            )
            .await,
            ControlFlow::Continue(())
        );
        assert!(result.await.unwrap().is_none());

        let mut replace = fixture().await;
        let shared = replace.inner.shared.clone();
        let context = Arc::new(PathFence::default());
        let fence = shared.fences.begin(context.clone()).unwrap();
        let payload = shared.admission.reserve_payload(123).unwrap();
        assert_eq!(
            fail_work(
                &mut replace,
                Work::Replace {
                    path: Arc::from("db/replace"),
                    revision: b"r1".to_vec(),
                    body: b"body".to_vec(),
                    current_after: point(1),
                    fence,
                    payload,
                },
            )
            .await,
            ControlFlow::Continue(())
        );
        assert!(!context.is_active());

        let mut invalidate = fixture().await;
        let shared = invalidate.inner.shared.clone();
        let context = Arc::new(PathFence::default());
        let fence = shared.fences.begin(context.clone()).unwrap();
        assert_eq!(
            fail_work(
                &mut invalidate,
                Work::Invalidate {
                    path: Arc::from("db/invalidate"),
                    fence,
                },
            )
            .await,
            ControlFlow::Continue(())
        );
        assert!(!context.is_active());

        let mut promote = fixture().await;
        let shared = promote.inner.shared.clone();
        let path = Arc::<str>::from("db/promote");
        let optional = shared.admission.reserve_optional().unwrap();
        let promotion = shared.admission.reserve_promotion(&path).unwrap();
        let context = Arc::new(PathFence::default());
        let (epoch, active) = context.snapshot();
        assert!(!active);
        assert_eq!(
            fail_work(
                &mut promote,
                Work::Promote {
                    context,
                    epoch,
                    optional,
                    promotion,
                },
            )
            .await,
            ControlFlow::Continue(())
        );

        let mut shutdown = fixture().await;
        assert_eq!(
            fail_work(&mut shutdown, Work::Shutdown).await,
            ControlFlow::Break(())
        );
    }

    #[tokio::test]
    async fn cancelling_each_handler_releases_all_work_reservations() {
        let mut lookup = fixture().await;
        let shared = lookup.inner.shared.clone();
        let optional = shared.admission.reserve_optional().unwrap();
        let (completion, result) = oneshot::channel();
        cancel_work(
            &mut lookup,
            Work::Lookup {
                path: Arc::from("db/lookup"),
                completion,
                optional,
            },
            (0, 0, 0),
            0,
        )
        .await;
        assert!(result.await.is_err());

        let mut replace = fixture().await;
        let shared = replace.inner.shared.clone();
        let context = Arc::new(PathFence::default());
        let fence = shared.fences.begin(context.clone()).unwrap();
        let payload = shared.admission.reserve_payload(123).unwrap();
        cancel_work(
            &mut replace,
            Work::Replace {
                path: Arc::from("db/replace"),
                revision: b"r1".to_vec(),
                body: b"body".to_vec(),
                current_after: point(1),
                fence,
                payload,
            },
            (0, 0, 0),
            1,
        )
        .await;
        assert!(!context.is_active());

        let mut invalidate = fixture().await;
        let shared = invalidate.inner.shared.clone();
        let context = Arc::new(PathFence::default());
        let fence = shared.fences.begin(context.clone()).unwrap();
        cancel_work(
            &mut invalidate,
            Work::Invalidate {
                path: Arc::from("db/invalidate"),
                fence,
            },
            (0, 0, 0),
            1,
        )
        .await;
        assert!(!context.is_active());

        let mut promote = fixture().await;
        let shared = promote.inner.shared.clone();
        let path = Arc::<str>::from("db/promote");
        let optional = shared.admission.reserve_optional().unwrap();
        let promotion = shared.admission.reserve_promotion(&path).unwrap();
        let context = Arc::new(PathFence::default());
        let (epoch, active) = context.snapshot();
        assert!(!active);
        cancel_work(
            &mut promote,
            Work::Promote {
                context,
                epoch,
                optional,
                promotion,
            },
            (0, 0, 1),
            0,
        )
        .await;

        let mut shutdown = fixture().await;
        cancel_work(&mut shutdown, Work::Shutdown, (0, 0, 0), 0).await;
    }

    #[tokio::test]
    async fn cancelling_worker_drops_in_flight_and_queued_reservations() {
        let mut fixture = fixture().await;
        let shared = fixture.inner.shared.clone();
        let mut pause = fixture.media.pause_next_operation();

        let active_context = Arc::new(PathFence::default());
        let active_fence = shared.fences.begin(active_context.clone()).unwrap();
        let active_payload = shared.admission.reserve_payload(111).unwrap();
        fixture.inner.enqueue_required(Work::Replace {
            path: Arc::from("db/active"),
            revision: b"r1".to_vec(),
            body: b"body".to_vec(),
            current_after: point(1),
            fence: active_fence,
            payload: active_payload,
        });
        let worker = fixture.worker.take().unwrap();
        let task = tokio::spawn(worker.run());
        pause.wait_until_entered().await;

        let (lookup_completion, lookup_result) = oneshot::channel();
        assert!(
            fixture
                .inner
                .enqueue_optional(move |optional| Work::Lookup {
                    path: Arc::from("db/lookup"),
                    completion: lookup_completion,
                    optional,
                })
        );

        let replace_context = Arc::new(PathFence::default());
        let replace_fence = shared.fences.begin(replace_context.clone()).unwrap();
        let replace_payload = shared.admission.reserve_payload(123).unwrap();
        fixture.inner.enqueue_required(Work::Replace {
            path: Arc::from("db/replace"),
            revision: b"r2".to_vec(),
            body: b"queued".to_vec(),
            current_after: point(2),
            fence: replace_fence,
            payload: replace_payload,
        });

        let invalidate_context = Arc::new(PathFence::default());
        let invalidate_fence = shared.fences.begin(invalidate_context.clone()).unwrap();
        fixture.inner.enqueue_required(Work::Invalidate {
            path: Arc::from("db/invalidate"),
            fence: invalidate_fence,
        });

        let promotion_path = Arc::<str>::from("db/promote");
        let promotion = shared.admission.reserve_promotion(&promotion_path).unwrap();
        let promotion_context = Arc::new(PathFence::default());
        let (epoch, active) = promotion_context.snapshot();
        assert!(!active);
        assert!(
            fixture
                .inner
                .enqueue_optional(move |optional| Work::Promote {
                    context: promotion_context,
                    epoch,
                    optional,
                    promotion,
                })
        );

        assert_eq!(shared.admission.reservation_counts(), (123, 2, 1));
        assert_eq!(shared.fences.active_count(), 3);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        pause.resume();

        assert_no_reservations(&shared);
        assert!(!active_context.is_active());
        assert!(!replace_context.is_active());
        assert!(!invalidate_context.is_active());
        assert!(lookup_result.await.is_err());
    }
}
