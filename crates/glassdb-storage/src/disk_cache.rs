//! Best-effort persistent encoded-body disk cache (ADR-045, ADR-048).

use std::collections::HashSet;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

mod disk;
mod file_media;
mod format;
mod media;
#[cfg(all(feature = "sim", sim))]
pub(crate) mod sim_harness;
#[cfg(any(test, feature = "sim"))]
pub(crate) mod sim_media;

use disk::{Disk, WriterState, open_disk};
use file_media::FileMedia;
use format::{CacheGeometry, PRODUCTION_GEOMETRY};
use media::CacheMedia;

const CACHE_FILE: &str = "l2.cache";
const FILTER_BYTES: usize = 4 * 1024 * 1024;
const FILTER_HIT_EPOCH: u64 = 1 << 20;
const WORK_QUEUE_ITEMS: usize = 4096;
const OPTIONAL_QUEUE_ITEMS: usize = 3072;
const MAX_ACTIVE_FENCES: usize = 4096;
const MAX_QUEUED_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_secs(5);
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
// One token per seven admission bytes bounds promotions to one eighth of
// combined append traffic; the segment cap also limits short bursts.
const PROMOTION_EARN_DIVISOR: u64 = 7;
const PROMOTION_CAP_DIVISOR: u64 = 8;

/// Configuration for the optional persistent encoded-body cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentCacheConfig {
    /// Directory containing the cache's `l2.cache` file.
    pub directory: PathBuf,
    /// Maximum file size, rounded down to the cache block size. Production
    /// caches require at least 131 MiB; 512 MiB or more is recommended.
    pub capacity_bytes: u64,
}

/// Opaque media selection for a persistent cache.
///
/// Callers normally omit this value to use native file media. Simulation media
/// constructs a handle carrying its compact cache geometry.
#[derive(Clone)]
pub struct PersistentCacheMedia {
    media: Arc<dyn CacheMedia>,
    geometry: CacheGeometry,
}

#[derive(Clone)]
pub struct PersistentCache {
    inner: Option<Arc<CacheInner>>,
    metrics: Arc<CacheMetrics>,
}

/// Result of opening a persistent cache.
pub struct OpenedPersistentCache {
    /// The opened cache, possibly disabled when initialization failed.
    pub cache: PersistentCache,
    /// The greatest sequence point recovered while opening the cache. The
    /// database timeline must start after this point before using `cache`.
    pub last_sequence_point: Option<SequencePoint>,
}

pub(crate) struct EncodedBody {
    pub(crate) revision: Vec<u8>,
    pub(crate) body: Vec<u8>,
    pub(crate) current_after: SequencePoint,
}

impl PersistentCache {
    /// Opens a best-effort persistent cache and reports the sequence point
    /// recovered during initialization without blocking the async runtime.
    ///
    /// Initialization failures disable the returned cache and are reported
    /// through tracing and cache statistics.
    pub async fn open(
        config: PersistentCacheConfig,
        database_name: &str,
        database_id: DatabaseId,
        media: Option<PersistentCacheMedia>,
    ) -> OpenedPersistentCache {
        let media = media.unwrap_or_else(|| PersistentCacheMedia {
            media: Arc::new(FileMedia),
            geometry: PRODUCTION_GEOMETRY,
        });
        Self::open_on_media(
            config,
            database_name,
            database_id,
            media.geometry,
            media.media,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_test_geometry(
        config: PersistentCacheConfig,
        database_name: &str,
        database_id: DatabaseId,
    ) -> OpenedPersistentCache {
        Self::open_with_geometry(
            config,
            database_name,
            database_id,
            format::COMPACT_GEOMETRY,
            Arc::new(CacheMetrics::new()),
            Arc::new(FileMedia),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_sim_media(
        config: PersistentCacheConfig,
        database_name: &str,
        database_id: DatabaseId,
        media: sim_media::SimMedia,
    ) -> OpenedPersistentCache {
        Self::open_with_geometry(
            config,
            database_name,
            database_id,
            format::COMPACT_GEOMETRY,
            Arc::new(CacheMetrics::new()),
            Arc::new(media),
        )
        .await
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.shared.enabled.load(Ordering::Acquire))
    }

    pub(crate) fn metrics(&self) -> Arc<CacheMetrics> {
        self.metrics.clone()
    }

    pub(crate) async fn lookup(&self, path: Arc<str>) -> Option<EncodedBody> {
        let inner = self.inner.as_ref()?;
        if u32::try_from(path.len()).is_err() {
            self.metrics.l2_miss();
            return None;
        }
        let (completion, result) = oneshot::channel();
        inner
            .enqueue_optional(Work::Lookup { path, completion })
            .then_some(())?;
        match result.await {
            Ok(encoded) => encoded,
            Err(_) => {
                inner.disable_message("persistent-cache worker stopped during lookup");
                None
            }
        }
    }

    pub(crate) fn begin_fence(&self, context: Arc<dyn FenceContext>) -> Option<FenceGuard> {
        let inner = self.inner.as_ref()?;
        if !inner.shared.enabled.load(Ordering::Acquire) {
            return None;
        }
        let reserved = inner.shared.active_fences.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| (current < MAX_ACTIVE_FENCES).then_some(current + 1),
        );
        if reserved.is_err() {
            inner.disable_message("persistent-cache path-fence capacity exhausted");
            return None;
        }
        let epoch = context.fence().begin();
        Some(FenceGuard {
            context,
            epoch,
            active_fences: inner.shared.active_fences.clone(),
        })
    }

    pub(crate) fn disable_slow_lookup(&self) {
        if let Some(inner) = &self.inner {
            inner.disable_message("persistent-cache lookup timed out");
        }
    }

    pub(crate) fn reject_corrupt_candidate(&self, path: Arc<str>, context: Arc<dyn FenceContext>) {
        self.metrics.l2_error();
        if let Some(guard) = self.begin_fence(context) {
            self.invalidate(path, guard);
        }
    }

    pub(crate) fn replace(
        &self,
        path: Arc<str>,
        revision: Vec<u8>,
        body: Vec<u8>,
        current_after: SequencePoint,
        fence: FenceGuard,
    ) {
        let Some(inner) = &self.inner else {
            return;
        };
        if u32::try_from(path.len()).is_err() {
            self.invalidate(path, fence);
            return;
        }
        let format = &inner.shared.disk.format;
        let size = match format.record_bytes(revision.len(), body.len()) {
            Some(size) if size <= format.maximum_record_bytes() => size,
            _ => {
                self.invalidate(path, fence);
                return;
            }
        };
        let Some(payload) = PayloadReservation::reserve(&inner.shared, size) else {
            self.invalidate(path, fence);
            return;
        };
        inner.enqueue_required(Work::Replace {
            path,
            revision,
            body,
            current_after,
            fence,
            _payload: payload,
        });
    }

    pub(crate) fn invalidate(&self, path: Arc<str>, fence: FenceGuard) {
        let Some(inner) = &self.inner else {
            return;
        };
        if u32::try_from(path.len()).is_err() {
            return;
        }
        inner.enqueue_required(Work::Invalidate { path, fence });
    }

    pub(crate) fn record_present_hit(&self, path: &Arc<str>, context: Arc<dyn FenceContext>) {
        let Some(inner) = &self.inner else {
            return;
        };
        let fence = context.fence();
        if !inner.shared.enabled.load(Ordering::Acquire)
            || fence.is_active()
            || u32::try_from(path.len()).is_err()
        {
            return;
        }
        let fingerprint = inner.shared.disk.path_fingerprint(path);
        if !inner.shared.filter.observe(fingerprint) {
            return;
        }
        let (epoch, active) = fence.snapshot();
        if active {
            return;
        }
        {
            let mut queued = inner.shared.promotions.lock().unwrap();
            if queued.contains(path.as_ref()) {
                return;
            }
            if queued.len() >= OPTIONAL_QUEUE_ITEMS {
                return;
            }
            queued.insert(path.clone());
        }
        let _ = inner.enqueue_optional(Work::Promote {
            path: path.clone(),
            context,
            epoch,
        });
    }

    pub(crate) async fn shutdown(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        let shutdown = async {
            if inner
                .shutdown_started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                inner
                    .shared
                    .shutdown_requested
                    .store(true, Ordering::Release);
                // A full queue already wakes the worker; the sentinel is only
                // needed to wake an idle receiver.
                inner.wake_for_shutdown();
            }
            inner.completion.wait().await;
            inner.join_worker().await;
        };
        if rt::timeout(SHUTDOWN_TIMEOUT, shutdown).await.is_err() {
            inner.shared.disable();
            inner.abort_worker();
            tracing::warn!("persistent cache shutdown timed out; aborting its worker");
        }
    }

    async fn open_on_media(
        config: PersistentCacheConfig,
        database_name: &str,
        database_id: DatabaseId,
        geometry: CacheGeometry,
        media: Arc<dyn CacheMedia>,
    ) -> OpenedPersistentCache {
        let metrics = Arc::new(CacheMetrics::new());
        let fallback_metrics = metrics.clone();
        let (completion, result) = oneshot::channel();
        let worker = match Self::spawn_worker(
            config,
            database_name.to_owned(),
            database_id,
            geometry,
            metrics,
            media,
            completion,
        ) {
            Ok(worker) => worker,
            Err(error) => {
                fallback_metrics.l2_error();
                tracing::warn!(%error, "persistent-cache worker failed to start");
                return Self::disabled_open(fallback_metrics);
            }
        };

        match rt::timeout(OPEN_TIMEOUT, result).await {
            Ok(Ok(opened)) => Self::attach_worker(opened, worker),
            Ok(Err(_)) => {
                worker.abort();
                fallback_metrics.l2_error();
                tracing::warn!("persistent-cache worker stopped during initialization");
                Self::disabled_open(fallback_metrics)
            }
            Err(_) => {
                worker.abort();
                fallback_metrics.l2_error();
                tracing::warn!("persistent-cache initialization timed out");
                Self::disabled_open(fallback_metrics)
            }
        }
    }

    #[cfg(test)]
    async fn open_with_geometry(
        config: PersistentCacheConfig,
        database_name: &str,
        database_id: DatabaseId,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
        media: Arc<dyn CacheMedia>,
    ) -> OpenedPersistentCache {
        let fallback_metrics = metrics.clone();
        let (completion, result) = oneshot::channel();
        let worker = Self::spawn_worker(
            config,
            database_name.to_owned(),
            database_id,
            geometry,
            metrics,
            media,
            completion,
        );
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                fallback_metrics.l2_error();
                tracing::warn!(%error, "persistent-cache worker failed to start");
                return Self::disabled_open(fallback_metrics);
            }
        };
        match result.await {
            Ok(opened) => Self::attach_worker(opened, worker),
            Err(_) => {
                worker.abort();
                fallback_metrics.l2_error();
                tracing::warn!("persistent-cache worker stopped during initialization");
                Self::disabled_open(fallback_metrics)
            }
        }
    }

    fn disabled(metrics: Arc<CacheMetrics>) -> Self {
        Self {
            inner: None,
            metrics,
        }
    }

    fn disabled_open(metrics: Arc<CacheMetrics>) -> OpenedPersistentCache {
        OpenedPersistentCache {
            cache: Self::disabled(metrics),
            last_sequence_point: None,
        }
    }

    fn attach_worker(
        opened: OpenedPersistentCache,
        worker: rt::DedicatedJoinHandle<()>,
    ) -> OpenedPersistentCache {
        if let Some(inner) = &opened.cache.inner {
            inner.attach_worker(worker);
        }
        opened
    }

    fn spawn_worker(
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

#[derive(Default)]
pub(crate) struct PathFence {
    state: Mutex<FenceState>,
}

/// Provides the path fence and retains its semantic owner while queued L2 work
/// can still refer to it.
pub(crate) trait FenceContext: Send + Sync {
    fn fence(&self) -> &PathFence;
}

impl FenceContext for PathFence {
    fn fence(&self) -> &PathFence {
        self
    }
}

#[derive(Default)]
struct FenceState {
    epoch: u64,
    active: bool,
}

impl PathFence {
    pub(crate) fn is_active(&self) -> bool {
        self.state.lock().unwrap().active
    }

    fn begin(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.epoch = state.epoch.wrapping_add(1);
        if state.epoch == 0 {
            state.epoch = 1;
        }
        state.active = true;
        state.epoch
    }

    fn snapshot(&self) -> (u64, bool) {
        let state = self.state.lock().unwrap();
        (state.epoch, state.active)
    }

    fn finish(&self, epoch: u64) {
        let mut state = self.state.lock().unwrap();
        if state.epoch == epoch {
            state.active = false;
        }
    }
}

pub(crate) struct FenceGuard {
    context: Arc<dyn FenceContext>,
    epoch: u64,
    active_fences: Arc<AtomicUsize>,
}

impl FenceGuard {
    fn is_current(&self) -> bool {
        self.context.fence().snapshot() == (self.epoch, true)
    }
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        self.context.fence().finish(self.epoch);
        self.active_fences.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CacheInner {
    shared: Arc<Shared>,
    sender: mpsc::Sender<Work>,
    // Orders producers against shutdown so no work enters after shutdown starts.
    enqueue_gate: Mutex<()>,
    shutdown_started: AtomicBool,
    completion: Arc<Completion>,
    worker_joined: Completion,
    worker: Mutex<Option<rt::DedicatedJoinHandle<()>>>,
}

impl CacheInner {
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
            filter: writer.filter.clone(),
            promotions: Mutex::new(HashSet::new()),
            active_fences: Arc::new(AtomicUsize::new(0)),
            queued_payload_bytes: AtomicU64::new(0),
            optional_queued: AtomicUsize::new(0),
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

    fn abort_worker(&self) {
        if let Some(worker) = self.worker.lock().unwrap().as_ref() {
            worker.abort();
        }
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

    fn enqueue_required(&self, work: Work) {
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

    fn enqueue_optional(&self, work: Work) -> bool {
        let _gate = self.enqueue_gate.lock().unwrap();
        if self.shared.shutdown_requested.load(Ordering::Acquire)
            || !self.shared.enabled.load(Ordering::Acquire)
        {
            work.remove_promotion(&self.shared);
            return false;
        }
        let reserved = self.shared.optional_queued.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| (current < OPTIONAL_QUEUE_ITEMS).then_some(current + 1),
        );
        if reserved.is_err() {
            work.remove_promotion(&self.shared);
            return false;
        }
        match self.sender.try_send(work) {
            Ok(()) => true,
            Err(error) => {
                self.shared.optional_queued.fetch_sub(1, Ordering::AcqRel);
                let work = match error {
                    TrySendError::Full(work) | TrySendError::Closed(work) => work,
                };
                work.remove_promotion(&self.shared);
                false
            }
        }
    }

    fn wake_for_shutdown(&self) {
        let _gate = self.enqueue_gate.lock().unwrap();
        let _ = self.sender.try_send(Work::Shutdown);
    }

    fn disable_message(&self, message: &'static str) {
        if self.shared.disable() {
            tracing::warn!("{message}");
        }
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

struct Shared {
    disk: Arc<Disk>,
    enabled: AtomicBool,
    metrics: Arc<CacheMetrics>,
    filter: Arc<HitFilter>,
    promotions: Mutex<HashSet<Arc<str>>>,
    active_fences: Arc<AtomicUsize>,
    queued_payload_bytes: AtomicU64,
    optional_queued: AtomicUsize,
    shutdown_requested: AtomicBool,
}

impl Shared {
    fn disable(&self) -> bool {
        if self.enabled.swap(false, Ordering::AcqRel) {
            self.metrics.l2_error();
            true
        } else {
            false
        }
    }
}

struct PayloadReservation {
    shared: Arc<Shared>,
    bytes: u64,
}

impl PayloadReservation {
    fn reserve(shared: &Arc<Shared>, bytes: u64) -> Option<Self> {
        shared
            .queued_payload_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= MAX_QUEUED_PAYLOAD_BYTES)
            })
            .ok()?;
        Some(Self {
            shared: shared.clone(),
            bytes,
        })
    }
}

impl Drop for PayloadReservation {
    fn drop(&mut self) {
        self.shared
            .queued_payload_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Completion {
    done: AtomicBool,
    notify: Notify,
}

impl Completion {
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

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct CompletionGuard(Arc<Completion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

enum Work {
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
    fn remove_promotion(&self, shared: &Shared) {
        if let Work::Promote { path, .. } = self {
            shared.promotions.lock().unwrap().remove(path.as_ref());
        }
    }
}

struct HitFilter {
    cells: Box<[AtomicU8]>,
    hits: AtomicU64,
    segment_reinitializations: AtomicUsize,
    resetting: AtomicBool,
}

impl HitFilter {
    fn new() -> Self {
        let cells = std::iter::repeat_with(|| AtomicU8::new(0))
            .take(FILTER_BYTES)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            cells,
            hits: AtomicU64::new(0),
            segment_reinitializations: AtomicUsize::new(0),
            resetting: AtomicBool::new(false),
        }
    }

    fn observe(&self, fingerprint: u64) -> bool {
        let counters = FILTER_BYTES * 4;
        let first = fingerprint as usize % counters;
        let second = mix64(fingerprint ^ 0x9e37_79b9_7f4a_7c15) as usize % counters;
        let first_before = self.increment(first);
        let second_before = if first == second {
            first_before
        } else {
            self.increment(second)
        };
        let before = first_before.min(second_before);
        let hits = self.hits.fetch_add(1, Ordering::Relaxed) + 1;
        if hits >= FILTER_HIT_EPOCH {
            self.reset();
        }
        before == 1
    }

    fn note_segment_reinitialized(&self, segment_count: usize) {
        let threshold = segment_count.div_ceil(2);
        let count = self
            .segment_reinitializations
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if count >= threshold {
            self.reset();
        }
    }

    fn increment(&self, counter: usize) -> u8 {
        let byte_index = counter / 4;
        let shift = (counter % 4) * 2;
        let mask = 0b11 << shift;
        let cell = &self.cells[byte_index];
        let mut current = cell.load(Ordering::Relaxed);
        loop {
            let before = (current & mask) >> shift;
            let after = before.saturating_add(1).min(2);
            let next = (current & !mask) | (after << shift);
            match cell.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return before,
                Err(actual) => current = actual,
            }
        }
    }

    fn reset(&self) {
        if self
            .resetting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        for cell in &self.cells {
            cell.store(0, Ordering::Relaxed);
        }
        self.hits.store(0, Ordering::Relaxed);
        self.segment_reinitializations.store(0, Ordering::Relaxed);
        self.resetting.store(false, Ordering::Release);
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
                shared.optional_queued.fetch_sub(1, Ordering::AcqRel);
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
                shared.optional_queued.fetch_sub(1, Ordering::AcqRel);
                let result = if shared.enabled.load(Ordering::Acquire) {
                    promote(&shared, &mut writer, &path, context.fence(), epoch).await
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                shared.promotions.lock().unwrap().remove(path.as_ref());
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

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::format::COMPACT_GEOMETRY;
    use super::sim_media::{MediaFaultProfile, SimMedia};
    use super::*;
    use tempfile::TempDir;

    const TEST_CAPACITY: u64 = 2 * 1024 * 1024;
    const RECORD_CONTENT_OFFSET: u64 = 48;

    fn id(byte: u8) -> DatabaseId {
        DatabaseId::from_bytes([byte; 16])
    }

    async fn open(dir: &TempDir, database_id: DatabaseId) -> PersistentCache {
        open_result(dir, database_id).await.cache
    }

    async fn open_result(dir: &TempDir, database_id: DatabaseId) -> OpenedPersistentCache {
        PersistentCache::open_with_geometry(
            config(dir),
            "db",
            database_id,
            COMPACT_GEOMETRY,
            Arc::new(CacheMetrics::new()),
            Arc::new(FileMedia),
        )
        .await
    }

    async fn open_sim_result(
        dir: &TempDir,
        database_id: DatabaseId,
        media: SimMedia,
    ) -> OpenedPersistentCache {
        PersistentCache::open_with_sim_media(config(dir), "db", database_id, media).await
    }

    fn config(dir: &TempDir) -> PersistentCacheConfig {
        PersistentCacheConfig {
            directory: dir.path().to_path_buf(),
            capacity_bytes: TEST_CAPACITY,
        }
    }

    fn point(value: u64) -> SequencePoint {
        SequencePoint::from_raw(value)
    }

    fn assert_zero_padded_vector(label: &str, actual: &[u8], prefix: &[u8]) {
        assert_eq!(
            &actual[..prefix.len()],
            prefix,
            "{label} prefix drifted: {:02x?}",
            &actual[..prefix.len()]
        );
        assert!(
            actual[prefix.len()..].iter().all(|byte| *byte == 0),
            "{label} padding is not zero"
        );
    }

    fn publish(cache: &PersistentCache, path: &str, revision: &[u8], body: &[u8]) {
        publish_at(cache, path, revision, body, point(1));
    }

    fn publish_at(
        cache: &PersistentCache,
        path: &str,
        revision: &[u8],
        body: &[u8],
        current_after: SequencePoint,
    ) {
        let fence = Arc::new(PathFence::default());
        let guard = cache.begin_fence(fence).unwrap();
        cache.replace(
            Arc::from(path),
            revision.to_vec(),
            body.to_vec(),
            current_after,
            guard,
        );
    }

    #[tokio::test]
    async fn lookup_is_ordered_with_worker_writes() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish(&cache, "db/object", b"r1", b"body");

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.revision, b"r1");
        assert_eq!(record.body, b"body");

        cache.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_returns_when_media_operation_is_stalled() {
        let dir = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let cache = open_sim_result(&dir, id(1), media.clone()).await.cache;
        let inner = cache.inner.as_ref().unwrap().clone();
        let mut pause = media.pause_next_operation();
        publish(&cache, "db/object", b"r1", b"body");
        pause.wait_until_entered().await;

        cache.shutdown().await;
        assert!(!cache.is_enabled());

        pause.resume();
        inner.completion.wait().await;
    }

    #[tokio::test]
    async fn clean_reopen_preserves_record_and_identity_change_discards() {
        let dir = TempDir::new().unwrap();
        let first_id = id(1);
        let cache = open(&dir, first_id).await;
        assert!(cache.is_enabled());
        publish_at(&cache, "db/object", b"r1", b"body", point(17));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, first_id).await;
        assert_eq!(opened.last_sequence_point, Some(point(17)));
        let reopened = opened.cache;
        assert!(reopened.is_enabled());
        let got = reopened.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(got.revision, b"r1");
        assert_eq!(got.body, b"body");
        assert_eq!(got.current_after, point(17));
        reopened.shutdown().await;
        drop(reopened);

        let different = open(&dir, id(2)).await;
        assert!(different.lookup(Arc::from("db/object")).await.is_none());
        different.shutdown().await;
    }

    // Golden vectors for the compact persistent format. The expected bytes and
    // offsets are deliberately independent of the format helpers so any on-disk
    // change is explicit.
    #[tokio::test]
    async fn persistent_format_bytes_and_clean_tail_are_golden() {
        const HEADER_PREFIX: [u8; 80] = [
            0x47, 0x4c, 0x32, 0x54, 0x45, 0x53, 0x54, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xab, 0xa6, 0xbc, 0xa7, 0x95, 0x1d, 0x2a, 0xe1, 0xeb, 0x89, 0xbe, 0x6e,
            0x5f, 0xc5, 0xf3, 0xec, 0x78, 0x30, 0x91, 0x89, 0x0c, 0x8e, 0x38, 0xfd, 0x2b, 0xb5,
            0x7a, 0x55, 0xd0, 0x90, 0x46, 0xb3, 0xb2, 0x2a, 0xdd, 0x22, 0xe1, 0x63, 0x35, 0x0e,
            0x3e, 0x89, 0x5d, 0x36, 0x9d, 0x7e, 0x6f, 0x59, 0xcc, 0x08, 0xec, 0xdc, 0x42, 0xf0,
            0xff, 0xc9, 0x36, 0xba, 0xe4, 0x99, 0x5f, 0x87, 0x7c, 0x0a,
        ];
        const SLOT: [u8; 40] = [
            0xea, 0xb3, 0x14, 0x9e, 0xb7, 0xf7, 0x4a, 0x7a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xb0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        const MARKER_PREFIX: [u8; 48] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xdc, 0xaf, 0xa0, 0xb5, 0x62, 0xea, 0x64, 0xd8, 0xa8, 0xb4, 0x28, 0x8b,
            0x24, 0x0c, 0xeb, 0xc6, 0xf0, 0x03, 0xa2, 0x35, 0xa2, 0x29, 0x72, 0x66, 0xa2, 0xb5,
            0x03, 0xbc, 0x69, 0x1b, 0xcd, 0x79,
        ];
        const RECORD_PREFIX: [u8; 54] = [
            0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x49, 0xf2, 0x1a, 0xce, 0x4f, 0xbe, 0xfd, 0x52, 0x25, 0x8f, 0xc2, 0x4b,
            0x18, 0x3f, 0x52, 0xc6, 0xe7, 0xe3, 0x66, 0x8e, 0x13, 0x34, 0x7d, 0x56, 0xff, 0x2b,
            0x6a, 0x4f, 0xf1, 0x41, 0x59, 0xf2, 0x72, 0x31, 0x62, 0x6f, 0x64, 0x79,
        ];
        const HEADER_OFFSET: usize = 0;
        const MARKER_OFFSET: usize = 4 * 1024;
        const SLOT_OFFSET: usize = 16 * 1024;
        const RECORD_OFFSET: usize = 45_056;
        const CLEAN_TAIL: u64 = 49_152;

        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut writer, last_sequence_point) = open_disk(
            config(&dir),
            "db",
            database_id,
            COMPACT_GEOMETRY,
            metrics,
            Arc::new(media.clone()),
        )
        .await
        .unwrap();
        let block_bytes = disk.format.block_bytes() as usize;
        let minimum_record_bytes = disk.format.minimum_record_bytes() as usize;
        assert_eq!(last_sequence_point, None);
        let slot = writer
            .append("db/object", b"r1", b"body", point(17))
            .await
            .unwrap();
        assert_eq!(slot.record_offset, RECORD_OFFSET as u64);
        assert_eq!(slot.record_offset + slot.record_bytes, CLEAN_TAIL);
        writer.clean_shutdown().await.unwrap();

        let bytes = media.durable_bytes().unwrap();
        assert_zero_padded_vector(
            "persistent-cache header",
            &bytes[HEADER_OFFSET..HEADER_OFFSET + block_bytes],
            &HEADER_PREFIX,
        );
        assert_eq!(
            &bytes[SLOT_OFFSET..SLOT_OFFSET + SLOT.len()],
            SLOT,
            "persistent-cache slot drifted"
        );
        assert_zero_padded_vector(
            "persistent-cache clean-tail marker",
            &bytes[MARKER_OFFSET..MARKER_OFFSET + block_bytes],
            &MARKER_PREFIX,
        );
        assert_zero_padded_vector(
            "persistent-cache record",
            &bytes[RECORD_OFFSET..RECORD_OFFSET + minimum_record_bytes],
            &RECORD_PREFIX,
        );
        drop(writer);
        drop(disk);

        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut writer, last_sequence_point) = open_disk(
            config(&dir),
            "db",
            database_id,
            COMPACT_GEOMETRY,
            metrics,
            Arc::new(media),
        )
        .await
        .unwrap();
        assert_eq!(last_sequence_point, Some(point(17)));
        let record = disk.lookup("db/object").await.unwrap().unwrap();
        assert_eq!(record.revision, b"r1");
        assert_eq!(record.body, b"body");
        assert_eq!(record.current_after, point(17));
        let resumed = writer
            .append("db/next", b"r2", b"next", point(18))
            .await
            .unwrap();
        assert_eq!(resumed.record_offset, CLEAN_TAIL);
    }

    #[tokio::test]
    async fn reopen_scans_the_maximum_persisted_sequence_point() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let cache = open(&dir, database_id).await;
        publish_at(&cache, "db/low", b"r1", b"low", point(8));
        publish_at(&cache, "db/high", b"r2", b"high", point(34));
        publish_at(&cache, "db/middle", b"r3", b"middle", point(21));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, database_id).await;
        assert_eq!(opened.last_sequence_point, Some(point(34)));
        let reopened = opened.cache;
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_owner_is_disabled_without_disturbing_the_owner() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let owner = open(&dir, database_id).await;
        let contender = open(&dir, database_id).await;

        assert!(owner.is_enabled());
        assert!(!contender.is_enabled());
        let stats = contender.metrics.snapshot_and_reset();
        assert_eq!(stats.l2_errors, 1, "cache stats: {stats:?}");

        publish(&owner, "db/object", b"r1", b"body");
        owner.shutdown().await;
        drop(owner);
        let reopened = open(&dir, database_id).await;
        assert_eq!(
            reopened.lookup(Arc::from("db/object")).await.unwrap().body,
            b"body"
        );
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn damaged_record_is_a_miss() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let first = open(&dir, database_id).await;
        publish(&first, "db/object", b"r1", b"body");
        first.shutdown().await;
        drop(first);

        let reopened = open(&dir, database_id).await;
        let inner = reopened.inner.as_ref().unwrap();
        let slot = inner
            .shared
            .disk
            .current_slot("db/object")
            .await
            .unwrap()
            .unwrap();
        let mut damaged = [0u8; 1];
        inner
            .shared
            .disk
            .file
            .read_exact_at(&mut damaged, slot.record_offset + RECORD_CONTENT_OFFSET)
            .await
            .unwrap();
        damaged[0] ^= 0xff;
        inner
            .shared
            .disk
            .file
            .write_all_at(&damaged, slot.record_offset + RECORD_CONTENT_OFFSET)
            .await
            .unwrap();

        assert!(reopened.lookup(Arc::from("db/object")).await.is_none());
        let stats = reopened.metrics.snapshot_and_reset();
        assert_eq!(stats.l2_errors, 1, "cache stats: {stats:?}");
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn simulated_crash_never_fabricates_a_partially_published_record() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let media = SimMedia::new(MediaFaultProfile::Healthy, (0..=255).collect(), 1);
        let cache = open_sim_result(&dir, database_id, media.clone())
            .await
            .cache;
        publish_at(&cache, "db/object", b"r1", b"body", point(17));
        assert_eq!(
            cache.lookup(Arc::from("db/object")).await.unwrap().body,
            b"body"
        );

        media.crash();
        drop(cache);
        rt::yield_now().await;

        let opened = open_sim_result(&dir, database_id, media).await;
        assert!(
            opened
                .last_sequence_point
                .is_none_or(|recovered| recovered == point(17)),
            "recovery invented a sequence point"
        );
        if let Some(record) = opened.cache.lookup(Arc::from("db/object")).await {
            assert_eq!(record.revision, b"r1");
            assert_eq!(record.body, b"body");
            assert_eq!(record.current_after, point(17));
        }
        opened.cache.shutdown().await;
    }

    #[tokio::test]
    async fn simulated_corruption_of_each_format_region_is_fail_open() {
        #[derive(Clone, Copy, Debug)]
        enum Region {
            Header,
            CleanTail,
            IndexSlot,
            SegmentHeader,
            Record,
        }

        for region in [
            Region::Header,
            Region::CleanTail,
            Region::IndexSlot,
            Region::SegmentHeader,
            Region::Record,
        ] {
            let dir = TempDir::new().unwrap();
            let database_id = id(1);
            let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
            let cache = open_sim_result(&dir, database_id, media.clone())
                .await
                .cache;
            publish_at(&cache, "db/object", b"r1", b"body", point(17));
            let record = cache.lookup(Arc::from("db/object")).await.unwrap();
            assert_eq!(record.body, b"body");
            let disk = &cache.inner.as_ref().unwrap().shared.disk;
            let slot = disk.current_slot("db/object").await.unwrap().unwrap();
            let segment = disk
                .segment_generations
                .iter()
                .position(|generation| generation.load(Ordering::Acquire) == slot.generation)
                .unwrap();
            let clean_tail_offset = disk.format.block_bytes();
            let encoded_slot = disk.format.encode_slot(slot);
            let segment_header_offset = disk.format.segment_start(segment);
            cache.shutdown().await;
            drop(cache);

            let offset = match region {
                Region::Header => 0,
                Region::CleanTail => clean_tail_offset,
                Region::IndexSlot => media
                    .durable_bytes()
                    .unwrap()
                    .windows(encoded_slot.len())
                    .position(|window| window == encoded_slot)
                    .expect("published index slot was not durable")
                    as u64,
                Region::SegmentHeader => segment_header_offset,
                Region::Record => slot.record_offset + RECORD_CONTENT_OFFSET,
            };
            assert!(media.corrupt(offset, 0x80), "could not corrupt {region:?}");

            let reopened = open_sim_result(&dir, database_id, media).await.cache;
            if let Some(record) = reopened.lookup(Arc::from("db/object")).await {
                assert_eq!(record.revision, b"r1", "region: {region:?}");
                assert_eq!(record.body, b"body", "region: {region:?}");
                assert_eq!(record.current_after, point(17), "region: {region:?}");
            }
            reopened.shutdown().await;
        }
    }

    #[tokio::test]
    async fn segment_ring_reuses_the_oldest_segment() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        for index in 0..450 {
            publish(&cache, &format!("db/object-{index}"), b"r1", b"body");
        }

        assert!(cache.lookup(Arc::from("db/object-0")).await.is_none());
        assert_eq!(
            cache.lookup(Arc::from("db/object-449")).await.unwrap().body,
            b"body"
        );
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn full_index_bucket_evicts_its_oldest_pointer() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let disk = &cache.inner.as_ref().unwrap().shared.disk;
        let bucket_count = disk.format.bucket_count();
        let mut paths = Vec::new();
        let mut candidate = 0;
        while paths.len() <= 128 {
            let path = format!("db/collision-{candidate}");
            if disk.path_fingerprint(&path).is_multiple_of(bucket_count) {
                paths.push(path);
            }
            candidate += 1;
        }
        for path in &paths {
            publish(&cache, path, b"r1", b"body");
        }

        assert!(cache.lookup(Arc::from(paths[0].as_str())).await.is_none());
        assert_eq!(
            cache
                .lookup(Arc::from(paths.last().unwrap().as_str()))
                .await
                .unwrap()
                .body,
            b"body"
        );
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn record_larger_than_a_segment_is_not_admitted() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let segment_bytes = cache
            .inner
            .as_ref()
            .unwrap()
            .shared
            .disk
            .format
            .segment_bytes() as usize;
        publish(&cache, "db/oversized", b"r1", &vec![0; segment_bytes]);

        assert!(cache.lookup(Arc::from("db/oversized")).await.is_none());
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn fence_guard_retains_its_semantic_context_until_release() {
        struct TestContext {
            fence: PathFence,
            dropped: Arc<AtomicBool>,
        }

        impl FenceContext for TestContext {
            fn fence(&self) -> &PathFence {
                &self.fence
            }
        }

        impl Drop for TestContext {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let context = Arc::new(TestContext {
            fence: PathFence::default(),
            dropped: dropped.clone(),
        });
        let guard = cache.begin_fence(context.clone()).unwrap();

        drop(context);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(guard);
        assert!(dropped.load(Ordering::SeqCst));

        cache.shutdown().await;
    }

    #[tokio::test]
    async fn newer_path_epoch_cancels_an_older_admission() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let fence = Arc::new(PathFence::default());
        let older = cache.begin_fence(fence.clone()).unwrap();
        let newer = cache.begin_fence(fence.clone()).unwrap();

        cache.replace(
            Arc::from("db/object"),
            b"r1".to_vec(),
            b"old".to_vec(),
            point(1),
            older,
        );
        cache.replace(
            Arc::from("db/object"),
            b"r2".to_vec(),
            b"new".to_vec(),
            point(2),
            newer,
        );

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.revision, b"r2");
        assert_eq!(record.body, b"new");
        assert!(!fence.is_active());
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn unclean_reopen_keeps_completed_records_without_reusing_the_old_tail() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut writer, _) = open_disk(
            config(&dir),
            "db",
            database_id,
            COMPACT_GEOMETRY,
            metrics,
            Arc::new(media.clone()),
        )
        .await
        .unwrap();
        let old_slot = writer
            .append("db/object", b"r1", b"body", point(1))
            .await
            .unwrap();
        writer.sync().await.unwrap();
        let old_segment = writer.active_segment.unwrap();
        drop(writer);
        drop(disk);

        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut recovered, last_sequence_point) = open_disk(
            config(&dir),
            "db",
            database_id,
            COMPACT_GEOMETRY,
            metrics,
            Arc::new(media),
        )
        .await
        .unwrap();
        assert_eq!(last_sequence_point, Some(point(1)));
        assert_eq!(
            disk.lookup("db/object").await.unwrap().unwrap().body,
            b"body"
        );
        assert_eq!(recovered.active_segment, None);
        let recovered_slot = recovered
            .append("db/new", b"r2", b"new", point(2))
            .await
            .unwrap();
        assert_ne!(recovered.active_segment, Some(old_segment));
        assert_ne!(
            recovered_slot.record_offset,
            old_slot.record_offset + old_slot.record_bytes
        );
        assert_eq!(recovered_slot.record_offset, 307_200);
    }

    #[test]
    fn second_chance_filter_emits_once_on_the_second_hit_and_resets() {
        let filter = HitFilter::new();
        assert!(!filter.observe(42));
        assert!(filter.observe(42));
        assert!(!filter.observe(42));

        filter.note_segment_reinitialized(4);
        filter.note_segment_reinitialized(4);
        assert!(!filter.observe(42));
        assert!(filter.observe(42));
    }
}
