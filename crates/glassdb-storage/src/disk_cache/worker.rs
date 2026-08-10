//! Persistent-cache work queues, lifecycle, and worker policy.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

use super::admission::{Admission, PayloadReservation};
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

    pub(super) fn enqueue_optional(&self, work: Work) -> bool {
        let _gate = self.enqueue_gate.lock().unwrap();
        if self.shared.shutdown_requested.load(Ordering::Acquire)
            || !self.shared.enabled.load(Ordering::Acquire)
        {
            work.remove_promotion(&self.shared.admission);
            return false;
        }
        if !self.shared.admission.reserve_optional() {
            work.remove_promotion(&self.shared.admission);
            return false;
        }
        match self.sender.try_send(work) {
            Ok(()) => true,
            Err(error) => {
                self.shared.admission.release_optional();
                let work = match error {
                    TrySendError::Full(work) | TrySendError::Closed(work) => work,
                };
                work.remove_promotion(&self.shared.admission);
                false
            }
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
    },
    Replace {
        path: Arc<str>,
        revision: Vec<u8>,
        body: Vec<u8>,
        current_after: SequencePoint,
        // Drop releases the path only after publication finishes.
        fence: FenceGuard,
        _payload: PayloadReservation,
    },
    Invalidate {
        path: Arc<str>,
        // Drop releases the path only after invalidation finishes.
        fence: FenceGuard,
    },
    Promote {
        path: Arc<str>,
        context: Arc<dyn FenceContext>,
        epoch: u64,
    },
    Shutdown,
}

impl Work {
    fn remove_promotion(&self, admission: &Admission) {
        if let Work::Promote { path, .. } = self {
            admission.remove_promotion(path);
        }
    }
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
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                    clean_shutdown(&shared, &mut writer).await;
                    break;
                }
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
        let result = match work {
            Work::Lookup { path, completion } => {
                shared.admission.release_optional();
                let _ = completion.send(lookup(&shared, &path).await);
                Ok(())
            }
            Work::Replace {
                path,
                revision,
                body,
                current_after,
                fence,
                _payload: _,
            } => {
                let result = if shared.enabled.load(Ordering::Acquire) && fence.is_current() {
                    let result = writer.append(&path, &revision, &body, current_after).await;
                    if let Ok(slot) = result {
                        let earned = slot.record_bytes / PROMOTION_EARN_DIVISOR;
                        let cap = writer.disk.format.maximum_record_bytes() / PROMOTION_CAP_DIVISOR;
                        writer.promotion_tokens =
                            writer.promotion_tokens.saturating_add(earned).min(cap);
                    }
                    result.map(|_| ())
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                drop(fence);
                result
            }
            Work::Invalidate { path, fence } => {
                let result = if shared.enabled.load(Ordering::Acquire) {
                    writer.invalidate(&path).await
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                drop(fence);
                result
            }
            Work::Promote {
                path,
                context,
                epoch,
            } => {
                shared.admission.release_optional();
                let result = if shared.enabled.load(Ordering::Acquire) {
                    promote(&shared, &mut writer, &path, context.fence(), epoch).await
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                shared.admission.remove_promotion(&path);
                result
            }
            Work::Shutdown => {
                clean_shutdown(&shared, &mut writer).await;
                break;
            }
        };
        let _ = result;
        sync_writer_if_needed(&shared, &mut writer, &mut last_sync, false).await;
    }
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
