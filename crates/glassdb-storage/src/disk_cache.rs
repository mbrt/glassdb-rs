//! Best-effort persistent encoded-body disk cache (ADR-045).

use std::any::Any;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use glassdb_concurr::rt;
use glassdb_data::DatabaseUuid;
use rustix::fs::{FallocateFlags, FlockOperation};
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, oneshot};

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

const CACHE_FILE: &str = "l2.cache";
const SLOT_BYTES: u64 = 40;
const RECORD_HEADER_BYTES: u64 = 48;
const RECORD_ALIGNMENT: u64 = 8;
const INDEX_SCAN_BYTES: usize = 4 * 1024 * 1024;
const FILTER_BYTES: usize = 4 * 1024 * 1024;
const FILTER_HIT_EPOCH: u64 = 1 << 20;
const WORK_QUEUE_ITEMS: usize = 4096;
const OPTIONAL_QUEUE_ITEMS: usize = 3072;
const MAX_ACTIVE_FENCES: usize = 4096;
const MAX_QUEUED_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;
const SYNC_BYTES: u64 = 64 * 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_secs(5);
const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for the optional persistent encoded-body cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentCacheConfig {
    /// Directory containing the cache's `l2.cache` file.
    pub directory: PathBuf,
    /// Maximum file size, rounded down to the cache block size. Production
    /// caches require at least 131 MiB; 512 MiB or more is recommended.
    pub capacity_bytes: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct CacheGeometry {
    magic: [u8; 8],
    format_version: u64,
    block_bytes: u64,
    minimum_record_bytes: u64,
    segment_bytes: u64,
    index_divisor: u64,
    minimum_segments: u64,
    identity_domain: &'static [u8],
    header_domain: &'static [u8],
    marker_domain: &'static [u8],
    path_domain: &'static [u8],
    record_domain: &'static [u8],
}

pub(crate) const PRODUCTION_GEOMETRY: CacheGeometry = CacheGeometry {
    magic: *b"GLDBL2\0\0",
    format_version: 1,
    block_bytes: 4 * 1024,
    minimum_record_bytes: 4 * 1024,
    segment_bytes: 64 * 1024 * 1024,
    index_divisor: 64,
    minimum_segments: 2,
    identity_domain: b"glassdb-l2-identity-v1",
    header_domain: b"glassdb-l2-header-v1",
    marker_domain: b"glassdb-l2-clean-tail-v1",
    path_domain: b"glassdb-l2-path-v1",
    record_domain: b"glassdb-l2-record-v1",
};

#[cfg(test)]
pub(crate) const TEST_GEOMETRY: CacheGeometry = CacheGeometry {
    magic: *b"GL2TEST\0",
    format_version: 1,
    block_bytes: 4 * 1024,
    minimum_record_bytes: 4 * 1024,
    segment_bytes: 256 * 1024,
    index_divisor: 64,
    minimum_segments: 2,
    identity_domain: b"glassdb-l2-identity-test-v1",
    header_domain: b"glassdb-l2-header-test-v1",
    marker_domain: b"glassdb-l2-clean-tail-test-v1",
    path_domain: b"glassdb-l2-path-test-v1",
    record_domain: b"glassdb-l2-record-test-v1",
};

#[derive(Clone, Copy, Debug)]
struct Layout {
    capacity: u64,
    metadata_bytes: u64,
    index_bytes: u64,
    data_offset: u64,
    segment_count: usize,
    ring_end: u64,
}

impl Layout {
    fn derive(capacity: u64, geometry: CacheGeometry) -> io::Result<Self> {
        if geometry.block_bytes < 4096
            || !geometry.block_bytes.is_power_of_two()
            || geometry.minimum_record_bytes < RECORD_HEADER_BYTES
            || !geometry
                .minimum_record_bytes
                .is_multiple_of(RECORD_ALIGNMENT)
            || geometry.segment_bytes <= geometry.block_bytes
            || !geometry.segment_bytes.is_multiple_of(geometry.block_bytes)
            || geometry.index_divisor == 0
            || geometry.minimum_segments < 2
            || geometry.block_bytes < SLOT_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid persistent-cache geometry",
            ));
        }
        let capacity = floor_to(capacity, geometry.block_bytes);
        let metadata_bytes = geometry.block_bytes.checked_mul(2).ok_or_else(overflow)?;
        let index_bytes = floor_to(capacity / geometry.index_divisor, geometry.block_bytes);
        if index_bytes < geometry.block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity leaves no index bucket",
            ));
        }
        let data_offset = metadata_bytes
            .checked_add(index_bytes)
            .ok_or_else(overflow)?;
        let data_bytes = capacity.checked_sub(data_offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity is smaller than its metadata",
            )
        })?;
        let segment_count_u64 = data_bytes / geometry.segment_bytes;
        if segment_count_u64 < geometry.minimum_segments {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity must hold at least two segments",
            ));
        }
        let segment_count = usize::try_from(segment_count_u64).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache segment count does not fit in memory",
            )
        })?;
        let ring_end = data_offset
            .checked_add(
                segment_count_u64
                    .checked_mul(geometry.segment_bytes)
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        Ok(Self {
            capacity,
            metadata_bytes,
            index_bytes,
            data_offset,
            segment_count,
            ring_end,
        })
    }

    fn bucket_count(self, geometry: CacheGeometry) -> u64 {
        self.index_bytes / geometry.block_bytes
    }

    fn segment_start(self, geometry: CacheGeometry, index: usize) -> u64 {
        self.data_offset + index as u64 * geometry.segment_bytes
    }
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
        database_uuid: DatabaseUuid,
    ) -> OpenedPersistentCache {
        Self::open_on_worker(config, database_name, database_uuid, PRODUCTION_GEOMETRY).await
    }

    #[cfg(test)]
    pub(crate) async fn open_with_test_geometry(
        config: PersistentCacheConfig,
        database_name: &str,
        database_uuid: DatabaseUuid,
    ) -> OpenedPersistentCache {
        Self::open_on_worker(config, database_name, database_uuid, TEST_GEOMETRY).await
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

    pub(crate) fn begin_fence(
        &self,
        fence: Arc<PathFence>,
        keepalive: Arc<dyn Any + Send + Sync>,
    ) -> Option<FenceGuard> {
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
        let epoch = fence.begin();
        Some(FenceGuard {
            fence,
            epoch,
            active_fences: inner.shared.active_fences.clone(),
            _keepalive: keepalive,
        })
    }

    pub(crate) fn disable_slow_lookup(&self) {
        if let Some(inner) = &self.inner {
            inner.disable_message("persistent-cache lookup timed out");
        }
    }

    pub(crate) fn reject_corrupt_candidate(
        &self,
        path: Arc<str>,
        fence: Arc<PathFence>,
        keepalive: Arc<dyn Any + Send + Sync>,
    ) {
        self.metrics.l2_error();
        if let Some(guard) = self.begin_fence(fence, keepalive) {
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
        let size = match record_bytes(revision.len(), body.len(), inner.shared.disk.geometry) {
            Some(size)
                if size
                    <= inner.shared.disk.geometry.segment_bytes
                        - inner.shared.disk.geometry.block_bytes =>
            {
                size
            }
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

    pub(crate) fn record_present_hit(
        &self,
        path: &Arc<str>,
        fence: &Arc<PathFence>,
        keepalive: Arc<dyn Any + Send + Sync>,
    ) {
        let Some(inner) = &self.inner else {
            return;
        };
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
            fence: fence.clone(),
            epoch,
            keepalive,
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
        };
        if rt::timeout(SHUTDOWN_TIMEOUT, shutdown).await.is_err() {
            inner.shared.disable();
            tracing::warn!("persistent cache shutdown timed out; detaching its worker");
        }
    }

    async fn open_on_worker(
        config: PersistentCacheConfig,
        database_name: &str,
        database_uuid: DatabaseUuid,
        geometry: CacheGeometry,
    ) -> OpenedPersistentCache {
        let metrics = Arc::new(CacheMetrics::new());

        // Real filesystem activity cannot be replayed by the deterministic
        // executor. Fail open until the simulator has a modeled disk cache.
        if rt::in_sim() {
            tracing::warn!("persistent cache disabled in deterministic simulation");
            return Self::disabled_open(metrics);
        }

        let fallback_metrics = metrics.clone();
        let (completion, result) = oneshot::channel();
        if let Err(error) = Self::spawn_worker(
            config,
            database_name.to_owned(),
            database_uuid,
            geometry,
            metrics,
            completion,
        ) {
            fallback_metrics.l2_error();
            tracing::warn!(%error, "persistent-cache worker thread failed to start");
            return Self::disabled_open(fallback_metrics);
        }

        match rt::timeout(OPEN_TIMEOUT, result).await {
            Ok(Ok(cache)) => cache,
            Ok(Err(_)) => {
                fallback_metrics.l2_error();
                tracing::warn!("persistent-cache worker stopped during initialization");
                Self::disabled_open(fallback_metrics)
            }
            Err(_) => {
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
        database_uuid: DatabaseUuid,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
    ) -> OpenedPersistentCache {
        let fallback_metrics = metrics.clone();
        let (completion, result) = oneshot::channel();
        let started = Self::spawn_worker(
            config,
            database_name.to_owned(),
            database_uuid,
            geometry,
            metrics,
            completion,
        );
        if let Err(error) = started {
            fallback_metrics.l2_error();
            tracing::warn!(%error, "persistent-cache worker thread failed to start");
            return Self::disabled_open(fallback_metrics);
        }
        match result.await {
            Ok(opened) => opened,
            Err(_) => {
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

    fn spawn_worker(
        config: PersistentCacheConfig,
        database_name: String,
        database_uuid: DatabaseUuid,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
        completion: oneshot::Sender<OpenedPersistentCache>,
    ) -> io::Result<()> {
        std::thread::Builder::new()
            .name("glassdb-l2".to_string())
            .spawn(move || {
                Self::run_opening_worker(
                    config,
                    database_name,
                    database_uuid,
                    geometry,
                    metrics,
                    completion,
                );
            })
            .map(|_| ())
    }

    fn run_opening_worker(
        config: PersistentCacheConfig,
        database_name: String,
        database_uuid: DatabaseUuid,
        geometry: CacheGeometry,
        metrics: Arc<CacheMetrics>,
        completion: oneshot::Sender<OpenedPersistentCache>,
    ) {
        let (disk, writer, last_sequence_point) = match open_disk(
            config,
            &database_name,
            database_uuid,
            geometry,
            metrics.clone(),
        ) {
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
            worker.run();
        }
    }
}

#[derive(Default)]
pub(crate) struct PathFence {
    state: Mutex<FenceState>,
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
    fence: Arc<PathFence>,
    epoch: u64,
    active_fences: Arc<AtomicUsize>,
    // Path coordination is weakly indexed. Retaining its state ensures a later
    // operation finds this same fence until the queued change is complete.
    _keepalive: Arc<dyn Any + Send + Sync>,
}

impl FenceGuard {
    fn is_current(&self) -> bool {
        self.fence.snapshot() == (self.epoch, true)
    }
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        self.fence.finish(self.epoch);
        self.active_fences.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CacheInner {
    shared: Arc<Shared>,
    sender: SyncSender<Work>,
    enqueue_gate: Mutex<()>,
    shutdown_started: AtomicBool,
    completion: Arc<Completion>,
}

impl CacheInner {
    fn prepare(
        disk: Arc<Disk>,
        writer: WriterState,
        metrics: Arc<CacheMetrics>,
    ) -> (Self, CacheWorker) {
        let (sender, receiver) = mpsc::sync_channel(WORK_QUEUE_ITEMS);
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
            },
            CacheWorker {
                shared,
                writer,
                receiver,
                completion,
            },
        )
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
            Err(TrySendError::Disconnected(work)) => {
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
                    TrySendError::Full(work) | TrySendError::Disconnected(work) => work,
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

struct CacheWorker {
    shared: Arc<Shared>,
    writer: WriterState,
    receiver: Receiver<Work>,
    completion: Arc<Completion>,
}

impl CacheWorker {
    fn run(self) {
        let _completion = CompletionGuard(self.completion);
        run_worker(self.shared, self.writer, self.receiver);
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
    #[cfg(test)]
    Stall {
        entered: mpsc::Sender<()>,
        release: Receiver<()>,
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
        fence: Arc<PathFence>,
        epoch: u64,
        // Promotions need the same weak-map lifetime rule as required fences.
        keepalive: Arc<dyn Any + Send + Sync>,
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

struct Disk {
    file: File,
    geometry: CacheGeometry,
    layout: Layout,
    identity: [u8; 32],
    segment_generations: Box<[AtomicU64]>,
    metrics: Arc<CacheMetrics>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Slot {
    fingerprint: u64,
    generation: u64,
    record_offset: u64,
    record_bytes: u64,
    current_after: SequencePoint,
}

struct Record {
    revision: Vec<u8>,
    body: Vec<u8>,
    current_after: SequencePoint,
}

impl Disk {
    fn lookup(&self, path: &str) -> io::Result<Option<Record>> {
        let fingerprint = self.path_fingerprint(path);
        let mut slots = self.matching_slots(fingerprint)?;
        slots.sort_unstable_by_key(|slot| std::cmp::Reverse(slot.generation));
        for slot in slots {
            match self.read_record(path, slot) {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {}
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    self.metrics.l2_error();
                    tracing::warn!(path, %error, "discarding corrupt persistent-cache record");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn path_fingerprint(&self, path: &str) -> u64 {
        let mut digest = Sha256::new();
        digest.update(self.geometry.path_domain);
        digest.update(self.identity);
        digest.update((path.len() as u32).to_le_bytes());
        digest.update(path.as_bytes());
        let digest = digest.finalize();
        u64::from_le_bytes(digest[..8].try_into().unwrap())
    }

    fn matching_slots(&self, fingerprint: u64) -> io::Result<Vec<Slot>> {
        let bucket = fingerprint % self.layout.bucket_count(self.geometry);
        let offset = self.layout.metadata_bytes + bucket * self.geometry.block_bytes;
        let mut bytes = vec![0; self.geometry.block_bytes as usize];
        self.read_exact_at(&mut bytes, offset)?;
        let mut slots = Vec::new();
        for raw in bytes.chunks_exact(SLOT_BYTES as usize) {
            let slot = decode_slot(raw);
            if slot.fingerprint == fingerprint
                && slot.generation != 0
                && self.slot_range(slot).is_some()
            {
                slots.push(slot);
            }
        }
        Ok(slots)
    }

    fn current_slot(&self, path: &str) -> io::Result<Option<Slot>> {
        let fingerprint = self.path_fingerprint(path);
        let mut slots = self.matching_slots(fingerprint)?;
        slots.sort_unstable_by_key(|slot| std::cmp::Reverse(slot.generation));
        for slot in slots {
            if self.read_record(path, slot)?.is_some() {
                return Ok(Some(slot));
            }
        }
        Ok(None)
    }

    fn last_sequence_point(&self) -> io::Result<Option<SequencePoint>> {
        let block_bytes = usize::try_from(self.geometry.block_bytes).map_err(|_| overflow())?;
        let scan_bytes = INDEX_SCAN_BYTES / block_bytes * block_bytes;
        let scan_bytes = scan_bytes.max(block_bytes);
        let mut bytes = vec![0; scan_bytes];
        let mut offset = self.layout.metadata_bytes;
        let index_end = offset
            .checked_add(self.layout.index_bytes)
            .ok_or_else(overflow)?;
        let mut maximum = None;
        while offset < index_end {
            let remaining = usize::try_from(index_end - offset).map_err(|_| overflow())?;
            let read_bytes = remaining.min(bytes.len());
            self.read_exact_at(&mut bytes[..read_bytes], offset)?;
            for bucket in bytes[..read_bytes].chunks_exact(block_bytes) {
                for raw in bucket.chunks_exact(SLOT_BYTES as usize) {
                    let slot = decode_slot(raw);
                    if slot.generation != 0 && self.slot_range(slot).is_some() {
                        maximum = Some(
                            maximum.map_or(slot.current_after, |current: SequencePoint| {
                                current.max(slot.current_after)
                            }),
                        );
                    }
                }
            }
            offset += read_bytes as u64;
        }
        if maximum.is_some_and(|point| point.raw() == u64::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "persistent-cache sequence point exhausted",
            ));
        }
        Ok(maximum)
    }

    fn read_record(&self, path: &str, slot: Slot) -> io::Result<Option<Record>> {
        let Some(segment) = self.slot_range(slot) else {
            return Ok(None);
        };
        if self.segment_generations[segment].load(Ordering::Acquire) != slot.generation {
            return Ok(None);
        }
        let record_len = usize::try_from(slot.record_bytes).map_err(|_| invalid_record())?;
        let mut bytes = vec![0; record_len];
        self.read_exact_at(&mut bytes, slot.record_offset)?;
        if self.segment_generations[segment].load(Ordering::Acquire) != slot.generation {
            return Ok(None);
        }
        let revision_bytes = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let body_bytes = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let current_after =
            SequencePoint::from_raw(u64::from_le_bytes(bytes[8..16].try_into().unwrap()));
        if current_after != slot.current_after {
            return Err(invalid_record());
        }
        let expected_record_bytes =
            record_bytes(revision_bytes, body_bytes, self.geometry).ok_or_else(invalid_record)?;
        if expected_record_bytes != slot.record_bytes {
            return Err(invalid_record());
        }
        let content_end = (RECORD_HEADER_BYTES as usize)
            .checked_add(revision_bytes)
            .and_then(|end| end.checked_add(body_bytes))
            .filter(|end| *end <= bytes.len())
            .ok_or_else(invalid_record)?;
        let revision_end = RECORD_HEADER_BYTES as usize + revision_bytes;
        let revision = &bytes[RECORD_HEADER_BYTES as usize..revision_end];
        let body = &bytes[revision_end..content_end];
        let expected_digest = record_digest(
            self.geometry,
            &self.identity,
            path,
            &bytes[..16],
            revision,
            body,
        )?;
        if bytes[16..48] != expected_digest {
            return Err(invalid_record());
        }
        Ok(Some(Record {
            revision: revision.to_vec(),
            body: body.to_vec(),
            current_after,
        }))
    }

    fn slot_range(&self, slot: Slot) -> Option<usize> {
        if slot.current_after.raw() == 0
            || !slot.record_offset.is_multiple_of(RECORD_ALIGNMENT)
            || slot.record_bytes < self.geometry.minimum_record_bytes
            || !slot.record_bytes.is_multiple_of(RECORD_ALIGNMENT)
            || slot.record_bytes
                > self
                    .geometry
                    .segment_bytes
                    .checked_sub(self.geometry.block_bytes)?
            || slot.record_offset < self.layout.data_offset
        {
            return None;
        }
        let relative = slot.record_offset.checked_sub(self.layout.data_offset)?;
        let segment = usize::try_from(relative / self.geometry.segment_bytes).ok()?;
        if segment >= self.layout.segment_count {
            return None;
        }
        let start = self.layout.segment_start(self.geometry, segment);
        let content_start = start.checked_add(self.geometry.block_bytes)?;
        let end = slot.record_offset.checked_add(slot.record_bytes)?;
        let segment_end = start.checked_add(self.geometry.segment_bytes)?;
        if slot.record_offset < content_start || end > segment_end || end > self.layout.ring_end {
            return None;
        }
        let generation = self.segment_generations[segment].load(Ordering::Acquire);
        (generation == slot.generation && generation != 0).then_some(segment)
    }

    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(bytes, offset)?;
        self.metrics.l2_read(bytes.len());
        Ok(())
    }

    fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(bytes, offset)?;
        self.metrics.l2_write(bytes.len());
        Ok(())
    }
}

struct WriterState {
    disk: Arc<Disk>,
    filter: Arc<HitFilter>,
    active_segment: Option<usize>,
    append_offset: u64,
    next_generation: u64,
    dirty_bytes: u64,
    last_sync: Instant,
    promotion_tokens: u64,
}

impl WriterState {
    fn append(
        &mut self,
        path: &str,
        revision: &[u8],
        body: &[u8],
        current_after: SequencePoint,
    ) -> io::Result<Slot> {
        let record_bytes = record_bytes(revision.len(), body.len(), self.disk.geometry)
            .ok_or_else(invalid_record)?;
        self.ensure_space(record_bytes)?;
        let segment = self.active_segment.expect("ensure_space selects a segment");
        let generation = self.disk.segment_generations[segment].load(Ordering::Acquire);
        let mut record = vec![0; record_bytes as usize];
        record[0..4].copy_from_slice(&(revision.len() as u32).to_le_bytes());
        record[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
        record[8..16].copy_from_slice(&current_after.raw().to_le_bytes());
        let digest = record_digest(
            self.disk.geometry,
            &self.disk.identity,
            path,
            &record[..16],
            revision,
            body,
        )?;
        record[16..48].copy_from_slice(&digest);
        let revision_end = RECORD_HEADER_BYTES as usize + revision.len();
        record[RECORD_HEADER_BYTES as usize..revision_end].copy_from_slice(revision);
        record[revision_end..revision_end + body.len()].copy_from_slice(body);
        let slot = Slot {
            fingerprint: self.disk.path_fingerprint(path),
            generation,
            record_offset: self.append_offset,
            record_bytes,
            current_after,
        };
        self.disk.write_all_at(&record, self.append_offset)?;
        self.publish(slot)?;
        self.append_offset += record_bytes;
        self.dirty_bytes = self.dirty_bytes.saturating_add(record_bytes + SLOT_BYTES);
        Ok(slot)
    }

    fn invalidate(&mut self, path: &str) -> io::Result<()> {
        let fingerprint = self.disk.path_fingerprint(path);
        let bucket = fingerprint % self.disk.layout.bucket_count(self.disk.geometry);
        let bucket_offset =
            self.disk.layout.metadata_bytes + bucket * self.disk.geometry.block_bytes;
        let mut bytes = vec![0; self.disk.geometry.block_bytes as usize];
        self.disk.read_exact_at(&mut bytes, bucket_offset)?;
        let zero = [0u8; SLOT_BYTES as usize];
        for (index, raw) in bytes.chunks_exact(SLOT_BYTES as usize).enumerate() {
            let slot = decode_slot(raw);
            if slot.generation != 0 && slot.fingerprint == fingerprint {
                self.disk
                    .write_all_at(&zero, bucket_offset + index as u64 * SLOT_BYTES)?;
                self.dirty_bytes = self.dirty_bytes.saturating_add(SLOT_BYTES);
            }
        }
        Ok(())
    }

    fn sync_if_needed(&mut self, force_time: bool) -> io::Result<()> {
        if self.dirty_bytes == 0 {
            return Ok(());
        }
        if self.dirty_bytes >= SYNC_BYTES || force_time || self.last_sync.elapsed() >= SYNC_INTERVAL
        {
            self.disk.file.sync_data()?;
            self.dirty_bytes = 0;
            self.last_sync = Instant::now();
        }
        Ok(())
    }

    fn clean_shutdown(&mut self) -> io::Result<()> {
        self.disk.file.sync_data()?;
        let mut marker = vec![0; self.disk.geometry.block_bytes as usize];
        if let Some(segment) = self.active_segment {
            let generation = self.disk.segment_generations[segment].load(Ordering::Acquire);
            marker[0..8].copy_from_slice(&generation.to_le_bytes());
            marker[8..16].copy_from_slice(&self.append_offset.to_le_bytes());
            let digest = marker_digest(
                self.disk.geometry,
                &self.disk.identity,
                self.disk.layout.capacity,
                &marker[..16],
            );
            marker[16..48].copy_from_slice(&digest);
        }
        self.disk
            .write_all_at(&marker, self.disk.geometry.block_bytes)?;
        self.disk.file.sync_data()?;
        self.dirty_bytes = 0;
        Ok(())
    }

    fn ensure_space(&mut self, record_bytes: u64) -> io::Result<()> {
        if let Some(segment) = self.active_segment {
            let end = self.disk.layout.segment_start(self.disk.geometry, segment)
                + self.disk.geometry.segment_bytes;
            if self.append_offset + record_bytes <= end {
                return Ok(());
            }
        }
        self.initialize_segment()
    }

    fn initialize_segment(&mut self) -> io::Result<()> {
        let mut unused = None;
        let mut oldest = None;
        for (index, generation) in self.disk.segment_generations.iter().enumerate() {
            let generation = generation.load(Ordering::Acquire);
            if generation == 0 {
                unused.get_or_insert(index);
            } else if oldest.is_none_or(|(_, current)| generation < current) {
                oldest = Some((index, generation));
            }
        }
        let segment = match unused {
            Some(segment) => segment,
            None => oldest.expect("a valid layout has segments").0,
        };
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("persistent-cache segment generation exhausted"))?;
        let mut header = vec![0; self.disk.geometry.block_bytes as usize];
        header[0..8].copy_from_slice(&generation.to_le_bytes());
        header[8..16].copy_from_slice(&(!generation).to_le_bytes());
        let start = self.disk.layout.segment_start(self.disk.geometry, segment);
        self.disk.write_all_at(&header, start)?;
        self.disk.segment_generations[segment].store(generation, Ordering::Release);
        self.active_segment = Some(segment);
        self.append_offset = start + self.disk.geometry.block_bytes;
        self.dirty_bytes = self
            .dirty_bytes
            .saturating_add(self.disk.geometry.block_bytes);
        self.filter
            .note_segment_reinitialized(self.disk.layout.segment_count);
        Ok(())
    }

    fn publish(&mut self, slot: Slot) -> io::Result<()> {
        let bucket = slot.fingerprint % self.disk.layout.bucket_count(self.disk.geometry);
        let bucket_offset =
            self.disk.layout.metadata_bytes + bucket * self.disk.geometry.block_bytes;
        let mut bytes = vec![0; self.disk.geometry.block_bytes as usize];
        self.disk.read_exact_at(&mut bytes, bucket_offset)?;
        let zero = [0u8; SLOT_BYTES as usize];
        for (index, raw) in bytes.chunks_exact_mut(SLOT_BYTES as usize).enumerate() {
            let previous = decode_slot(raw);
            if previous.generation != 0 && previous.fingerprint == slot.fingerprint {
                self.disk
                    .write_all_at(&zero, bucket_offset + index as u64 * SLOT_BYTES)?;
                raw.fill(0);
                self.dirty_bytes = self.dirty_bytes.saturating_add(SLOT_BYTES);
            }
        }
        let mut empty = None;
        let mut stale = None;
        let mut oldest = None;
        for (index, raw) in bytes.chunks_exact(SLOT_BYTES as usize).enumerate() {
            let candidate = decode_slot(raw);
            if candidate.generation == 0 {
                empty.get_or_insert(index);
                continue;
            }
            if self.disk.slot_range(candidate).is_none() {
                stale.get_or_insert(index);
                continue;
            }
            if oldest
                .is_none_or(|(_, current): (usize, Slot)| candidate.generation < current.generation)
            {
                oldest = Some((index, candidate));
            }
        }
        let index = empty
            .or(stale)
            .unwrap_or_else(|| oldest.expect("a non-empty bucket has a replacement").0);
        self.disk.write_all_at(
            &encode_slot(slot),
            bucket_offset + index as u64 * SLOT_BYTES,
        )?;
        Ok(())
    }
}

fn open_disk(
    config: PersistentCacheConfig,
    database_name: &str,
    database_uuid: DatabaseUuid,
    geometry: CacheGeometry,
    metrics: Arc<CacheMetrics>,
) -> io::Result<(Arc<Disk>, WriterState, Option<SequencePoint>)> {
    let layout = Layout::derive(config.capacity_bytes, geometry)?;
    let identity = identity_digest(geometry, database_name, database_uuid)?;
    std::fs::create_dir_all(&config.directory)?;
    let path = config.directory.join(CACHE_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive)?;
    let current_len = file.metadata()?.len();
    let valid = current_len == layout.capacity
        && current_len != 0
        && header_valid(&file, geometry, layout, &identity, &metrics)?;
    if !valid {
        if current_len != 0 {
            tracing::info!(
                path = %path.display(),
                current_bytes = current_len,
                configured_bytes = layout.capacity,
                "reinitializing incompatible persistent-cache container"
            );
        }
        initialize_file(&file, geometry, layout, &identity, &metrics)?;
    }

    let mut generations = Vec::with_capacity(layout.segment_count);
    let mut maximum = 0;
    for segment in 0..layout.segment_count {
        let mut header = [0u8; 16];
        file.read_exact_at(&mut header, layout.segment_start(geometry, segment))?;
        metrics.l2_read(header.len());
        let generation = u64::from_le_bytes(header[0..8].try_into().unwrap());
        let complement = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let generation = if generation != 0 && complement == !generation {
            maximum = maximum.max(generation);
            generation
        } else {
            0
        };
        generations.push(AtomicU64::new(generation));
    }
    let next_generation = maximum
        .checked_add(1)
        .ok_or_else(|| io::Error::other("persistent-cache segment generation exhausted"))?;
    let disk = Arc::new(Disk {
        file,
        geometry,
        layout,
        identity,
        segment_generations: generations.into_boxed_slice(),
        metrics,
    });
    let last_sequence_point = if valid {
        disk.last_sequence_point()?
    } else {
        None
    };
    let clean_tail = read_clean_tail(&disk, maximum)?;
    let (active_segment, append_offset) = clean_tail.unwrap_or((None, 0));
    let writer = WriterState {
        disk: disk.clone(),
        filter: Arc::new(HitFilter::new()),
        active_segment,
        append_offset,
        next_generation,
        dirty_bytes: 0,
        last_sync: Instant::now(),
        promotion_tokens: 0,
    };
    Ok((disk, writer, last_sequence_point))
}

fn initialize_file(
    file: &File,
    geometry: CacheGeometry,
    layout: Layout,
    identity: &[u8; 32],
    metrics: &CacheMetrics,
) -> io::Result<()> {
    file.set_len(0)?;
    file.set_len(layout.capacity)?;
    rustix::fs::fallocate(file, FallocateFlags::empty(), 0, layout.capacity)?;
    let mut header = vec![0; geometry.block_bytes as usize];
    header[0..8].copy_from_slice(&geometry.magic);
    header[8..16].copy_from_slice(&geometry.format_version.to_le_bytes());
    header[16..48].copy_from_slice(identity);
    let digest = header_digest(geometry, &header[..48], layout.capacity);
    header[48..80].copy_from_slice(&digest);
    file.write_all_at(&header, 0)?;
    metrics.l2_write(header.len());
    file.sync_all()?;
    Ok(())
}

fn header_valid(
    file: &File,
    geometry: CacheGeometry,
    layout: Layout,
    identity: &[u8; 32],
    metrics: &CacheMetrics,
) -> io::Result<bool> {
    let mut header = vec![0; geometry.block_bytes as usize];
    file.read_exact_at(&mut header, 0)?;
    metrics.l2_read(header.len());
    if header[0..8] != geometry.magic
        || u64::from_le_bytes(header[8..16].try_into().unwrap()) != geometry.format_version
        || header[16..48] != identity[..]
    {
        return Ok(false);
    }
    Ok(header[48..80] == header_digest(geometry, &header[..48], layout.capacity))
}

fn read_clean_tail(disk: &Disk, maximum: u64) -> io::Result<Option<(Option<usize>, u64)>> {
    if maximum == 0 {
        return Ok(None);
    }
    let mut marker = vec![0; disk.geometry.block_bytes as usize];
    disk.read_exact_at(&mut marker, disk.geometry.block_bytes)?;
    let generation = u64::from_le_bytes(marker[0..8].try_into().unwrap());
    let append_offset = u64::from_le_bytes(marker[8..16].try_into().unwrap());
    if generation != maximum
        || marker[16..48]
            != marker_digest(
                disk.geometry,
                &disk.identity,
                disk.layout.capacity,
                &marker[..16],
            )
    {
        return Ok(None);
    }
    for segment in 0..disk.layout.segment_count {
        if disk.segment_generations[segment].load(Ordering::Acquire) != generation {
            continue;
        }
        let start = disk.layout.segment_start(disk.geometry, segment);
        if append_offset % RECORD_ALIGNMENT == 0
            && append_offset >= start + disk.geometry.block_bytes
            && append_offset <= start + disk.geometry.segment_bytes
        {
            return Ok(Some((Some(segment), append_offset)));
        }
    }
    Ok(None)
}

fn lookup(shared: &Shared, path: &str) -> Option<EncodedBody> {
    if !shared.enabled.load(Ordering::Acquire) {
        return None;
    }
    match shared.disk.lookup(path) {
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

fn run_worker(shared: Arc<Shared>, mut writer: WriterState, receiver: Receiver<Work>) {
    loop {
        let work = if shared.shutdown_requested.load(Ordering::Acquire) {
            match receiver.try_recv() {
                Ok(work) => work,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                    clean_shutdown(&shared, &mut writer);
                    break;
                }
            }
        } else {
            match receiver.recv_timeout(SYNC_INTERVAL) {
                Ok(work) => work,
                Err(RecvTimeoutError::Timeout) => {
                    if shared.enabled.load(Ordering::Acquire)
                        && let Err(error) = writer.sync_if_needed(true)
                        && shared.disable()
                    {
                        tracing::warn!(%error, "persistent cache disabled after sync failure");
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        };
        let result = match work {
            Work::Lookup { path, completion } => {
                shared.optional_queued.fetch_sub(1, Ordering::AcqRel);
                let _ = completion.send(lookup(&shared, &path));
                Ok(())
            }
            #[cfg(test)]
            Work::Stall { entered, release } => {
                let _ = entered.send(());
                let _ = release.recv();
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
                    let result = writer.append(&path, &revision, &body, current_after);
                    if let Ok(slot) = result {
                        let earned = slot.record_bytes / 7;
                        let cap = (writer.disk.geometry.segment_bytes
                            - writer.disk.geometry.block_bytes)
                            / 8;
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
                    writer.invalidate(&path)
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                drop(fence);
                result
            }
            Work::Promote {
                path,
                fence,
                epoch,
                keepalive,
            } => {
                shared.optional_queued.fetch_sub(1, Ordering::AcqRel);
                let result = if shared.enabled.load(Ordering::Acquire) {
                    promote(&shared, &mut writer, &path, &fence, epoch)
                } else {
                    Ok(())
                };
                disable_after_work_error(&shared, &result);
                shared.promotions.lock().unwrap().remove(path.as_ref());
                drop(keepalive);
                result
            }
            Work::Shutdown => {
                clean_shutdown(&shared, &mut writer);
                break;
            }
        };
        let _ = result;
        if shared.enabled.load(Ordering::Acquire)
            && let Err(error) = writer.sync_if_needed(false)
            && shared.disable()
        {
            tracing::warn!(%error, "persistent cache disabled after sync failure");
        }
    }
}

fn clean_shutdown(shared: &Shared, writer: &mut WriterState) {
    if shared.enabled.load(Ordering::Acquire)
        && let Err(error) = writer.clean_shutdown()
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

fn promote(
    shared: &Shared,
    writer: &mut WriterState,
    path: &str,
    fence: &PathFence,
    epoch: u64,
) -> io::Result<()> {
    if fence.snapshot() != (epoch, false) {
        return Ok(());
    }
    let Some(slot) = shared.disk.current_slot(path)? else {
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
    let Some(record) = shared.disk.read_record(path, slot)? else {
        return Ok(());
    };
    if fence.snapshot() != (epoch, false) || shared.disk.current_slot(path)? != Some(slot) {
        return Ok(());
    }
    let promoted = writer.append(path, &record.revision, &record.body, record.current_after)?;
    writer.promotion_tokens -= promoted.record_bytes;
    Ok(())
}

fn identity_digest(
    geometry: CacheGeometry,
    database_name: &str,
    database_uuid: DatabaseUuid,
) -> io::Result<[u8; 32]> {
    let name_len = u32::try_from(database_name.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "database name is too long for persistent-cache identity",
        )
    })?;
    let mut digest = Sha256::new();
    digest.update(geometry.identity_domain);
    digest.update(name_len.to_le_bytes());
    digest.update(database_name.as_bytes());
    digest.update(database_uuid.as_bytes());
    Ok(digest.finalize().into())
}

fn header_digest(geometry: CacheGeometry, header: &[u8], capacity: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(geometry.header_domain);
    digest.update(header);
    digest.update(capacity.to_le_bytes());
    digest.finalize().into()
}

fn marker_digest(
    geometry: CacheGeometry,
    identity: &[u8; 32],
    capacity: u64,
    marker: &[u8],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(geometry.marker_domain);
    digest.update(identity);
    digest.update(capacity.to_le_bytes());
    digest.update(marker);
    digest.finalize().into()
}

fn record_digest(
    geometry: CacheGeometry,
    identity: &[u8; 32],
    path: &str,
    header: &[u8],
    revision: &[u8],
    body: &[u8],
) -> io::Result<[u8; 32]> {
    let path_len = u32::try_from(path.len()).map_err(|_| invalid_record())?;
    let mut digest = Sha256::new();
    digest.update(geometry.record_domain);
    digest.update(identity);
    digest.update(path_len.to_le_bytes());
    digest.update(path.as_bytes());
    digest.update(header);
    digest.update(revision);
    digest.update(body);
    Ok(digest.finalize().into())
}

fn record_bytes(revision_bytes: usize, body_bytes: usize, geometry: CacheGeometry) -> Option<u64> {
    u32::try_from(revision_bytes).ok()?;
    u32::try_from(body_bytes).ok()?;
    let content = RECORD_HEADER_BYTES
        .checked_add(revision_bytes as u64)?
        .checked_add(body_bytes as u64)?;
    Some(align_up(content, RECORD_ALIGNMENT)?.max(geometry.minimum_record_bytes))
}

fn encode_slot(slot: Slot) -> [u8; SLOT_BYTES as usize] {
    let mut bytes = [0; SLOT_BYTES as usize];
    bytes[0..8].copy_from_slice(&slot.fingerprint.to_le_bytes());
    bytes[8..16].copy_from_slice(&slot.generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&slot.record_offset.to_le_bytes());
    bytes[24..32].copy_from_slice(&slot.record_bytes.to_le_bytes());
    bytes[32..40].copy_from_slice(&slot.current_after.raw().to_le_bytes());
    bytes
}

fn decode_slot(bytes: &[u8]) -> Slot {
    Slot {
        fingerprint: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        generation: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        record_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        record_bytes: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        current_after: SequencePoint::from_raw(u64::from_le_bytes(
            bytes[32..40].try_into().unwrap(),
        )),
    }
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|value| value / alignment * alignment)
}

fn floor_to(value: u64, alignment: u64) -> u64 {
    value / alignment * alignment
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "persistent-cache geometry overflows u64",
    )
}

fn invalid_record() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid persistent-cache record",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const TEST_CAPACITY: u64 = 2 * 1024 * 1024;

    fn uuid(byte: u8) -> DatabaseUuid {
        let mut bytes = [byte; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        DatabaseUuid::from_bytes(bytes).unwrap()
    }

    async fn open(dir: &TempDir, database_uuid: DatabaseUuid) -> PersistentCache {
        open_result(dir, database_uuid).await.cache
    }

    async fn open_result(dir: &TempDir, database_uuid: DatabaseUuid) -> OpenedPersistentCache {
        PersistentCache::open_with_geometry(
            config(dir),
            "db",
            database_uuid,
            TEST_GEOMETRY,
            Arc::new(CacheMetrics::new()),
        )
        .await
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
        let guard = cache.begin_fence(fence, Arc::new(())).unwrap();
        cache.replace(
            Arc::from(path),
            revision.to_vec(),
            body.to_vec(),
            current_after,
            guard,
        );
    }

    #[test]
    fn production_minimum_is_131_mib() {
        assert!(Layout::derive(130 * 1024 * 1024, PRODUCTION_GEOMETRY).is_err());
        assert!(Layout::derive(131 * 1024 * 1024, PRODUCTION_GEOMETRY).is_ok());
    }

    #[tokio::test]
    async fn lookup_is_ordered_with_worker_writes() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, uuid(1)).await;
        publish(&cache, "db/object", b"r1", b"body");

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.revision, b"r1");
        assert_eq!(record.body, b"body");

        cache.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_returns_when_worker_is_stalled() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, uuid(1)).await;
        let inner = cache.inner.as_ref().unwrap().clone();
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        inner.enqueue_required(Work::Stall {
            entered,
            release: release_rx,
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker did not enter the stalled operation");

        cache.shutdown().await;
        assert!(!cache.is_enabled());

        release.send(()).unwrap();
        inner.completion.wait().await;
    }

    #[tokio::test]
    async fn clean_reopen_preserves_record_and_identity_change_discards() {
        let dir = TempDir::new().unwrap();
        let first_uuid = uuid(1);
        let cache = open(&dir, first_uuid).await;
        assert!(cache.is_enabled());
        publish_at(&cache, "db/object", b"r1", b"body", point(17));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, first_uuid).await;
        assert_eq!(opened.last_sequence_point, Some(point(17)));
        let reopened = opened.cache;
        assert!(reopened.is_enabled());
        let got = reopened.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(got.revision, b"r1");
        assert_eq!(got.body, b"body");
        assert_eq!(got.current_after, point(17));
        reopened.shutdown().await;
        drop(reopened);

        let different = open(&dir, uuid(2)).await;
        assert!(different.lookup(Arc::from("db/object")).await.is_none());
        different.shutdown().await;
    }

    #[tokio::test]
    async fn reopen_scans_the_maximum_persisted_sequence_point() {
        let dir = TempDir::new().unwrap();
        let database_uuid = uuid(1);
        let cache = open(&dir, database_uuid).await;
        publish_at(&cache, "db/low", b"r1", b"low", point(8));
        publish_at(&cache, "db/high", b"r2", b"high", point(34));
        publish_at(&cache, "db/middle", b"r3", b"middle", point(21));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, database_uuid).await;
        assert_eq!(opened.last_sequence_point, Some(point(34)));
        let reopened = opened.cache;
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_owner_is_disabled_without_disturbing_the_owner() {
        let dir = TempDir::new().unwrap();
        let database_uuid = uuid(1);
        let owner = open(&dir, database_uuid).await;
        let contender = open(&dir, database_uuid).await;

        assert!(owner.is_enabled());
        assert!(!contender.is_enabled());
        let stats = contender.metrics.snapshot_and_reset();
        assert_eq!(stats.l2_errors, 1, "cache stats: {stats:?}");

        publish(&owner, "db/object", b"r1", b"body");
        owner.shutdown().await;
        drop(owner);
        let reopened = open(&dir, database_uuid).await;
        assert_eq!(
            reopened.lookup(Arc::from("db/object")).await.unwrap().body,
            b"body"
        );
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn damaged_record_is_a_miss() {
        let dir = TempDir::new().unwrap();
        let database_uuid = uuid(1);
        let first = open(&dir, database_uuid).await;
        publish(&first, "db/object", b"r1", b"body");
        first.shutdown().await;
        drop(first);

        let reopened = open(&dir, database_uuid).await;
        let inner = reopened.inner.as_ref().unwrap();
        let slot = inner
            .shared
            .disk
            .current_slot("db/object")
            .unwrap()
            .unwrap();
        let mut damaged = [0u8; 1];
        inner
            .shared
            .disk
            .file
            .read_exact_at(&mut damaged, slot.record_offset + RECORD_HEADER_BYTES)
            .unwrap();
        damaged[0] ^= 0xff;
        inner
            .shared
            .disk
            .file
            .write_all_at(&damaged, slot.record_offset + RECORD_HEADER_BYTES)
            .unwrap();

        assert!(reopened.lookup(Arc::from("db/object")).await.is_none());
        let stats = reopened.metrics.snapshot_and_reset();
        assert_eq!(stats.l2_errors, 1, "cache stats: {stats:?}");
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn segment_ring_reuses_the_oldest_segment() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, uuid(1)).await;
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
        let cache = open(&dir, uuid(1)).await;
        let disk = &cache.inner.as_ref().unwrap().shared.disk;
        let bucket_count = disk.layout.bucket_count(disk.geometry);
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
        let cache = open(&dir, uuid(1)).await;
        publish(
            &cache,
            "db/oversized",
            b"r1",
            &vec![0; TEST_GEOMETRY.segment_bytes as usize],
        );

        assert!(cache.lookup(Arc::from("db/oversized")).await.is_none());
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn newer_path_epoch_cancels_an_older_admission() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, uuid(1)).await;
        let fence = Arc::new(PathFence::default());
        let older = cache.begin_fence(fence.clone(), Arc::new(())).unwrap();
        let newer = cache.begin_fence(fence.clone(), Arc::new(())).unwrap();

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

    #[test]
    fn unclean_reopen_keeps_completed_records_without_reusing_the_old_tail() {
        let dir = TempDir::new().unwrap();
        let database_uuid = uuid(1);
        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut writer, _) =
            open_disk(config(&dir), "db", database_uuid, TEST_GEOMETRY, metrics).unwrap();
        writer
            .append("db/object", b"r1", b"body", point(1))
            .unwrap();
        let old_segment = writer.active_segment.unwrap();
        drop(writer);
        drop(disk);

        let metrics = Arc::new(CacheMetrics::new());
        let (disk, mut recovered, last_sequence_point) =
            open_disk(config(&dir), "db", database_uuid, TEST_GEOMETRY, metrics).unwrap();
        assert_eq!(last_sequence_point, Some(point(1)));
        assert_eq!(disk.lookup("db/object").unwrap().unwrap().body, b"body");
        assert_eq!(recovered.active_segment, None);
        recovered.append("db/new", b"r2", b"new", point(2)).unwrap();
        assert_ne!(recovered.active_segment, Some(old_segment));
    }

    #[test]
    fn test_format_is_not_a_production_file() {
        assert_ne!(TEST_GEOMETRY.magic, PRODUCTION_GEOMETRY.magic);
        assert_ne!(
            TEST_GEOMETRY.header_domain,
            PRODUCTION_GEOMETRY.header_domain
        );
    }

    #[test]
    fn record_size_is_charged_and_aligned() {
        assert_eq!(record_bytes(2, 4, TEST_GEOMETRY), Some(4096));
        assert_eq!(record_bytes(2, 4097, TEST_GEOMETRY), Some(4152));
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
