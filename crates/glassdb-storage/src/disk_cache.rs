//! Best-effort persistent encoded-body disk cache (ADR-045, ADR-048).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use tokio::sync::oneshot;

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

mod admission;
mod disk;
mod fence;
mod file_media;
mod format;
mod media;
#[cfg(all(feature = "sim", sim))]
pub(crate) mod sim_harness;
#[cfg(any(test, feature = "sim"))]
pub(crate) mod sim_media;
mod worker;

pub(crate) use fence::{FenceContext, FenceGuard, PathFence};
use file_media::FileMedia;
use format::{CacheGeometry, PRODUCTION_GEOMETRY};
use media::CacheMedia;
use worker::{CacheInner, Work};

const CACHE_FILE: &str = "l2.cache";
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
            .enqueue_optional(move |optional| Work::Lookup {
                path,
                completion,
                optional,
            })
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
        let Some(guard) = inner.shared.fences.begin(context) else {
            inner.disable_message("persistent-cache path-fence capacity exhausted");
            return None;
        };
        Some(guard)
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
        let Some(payload) = inner.shared.admission.reserve_payload(size) else {
            self.invalidate(path, fence);
            return;
        };
        inner.enqueue_required(Work::Replace {
            path,
            revision,
            body,
            current_after,
            fence,
            payload,
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
        if !inner.shared.admission.observe_hit(fingerprint) {
            return;
        }
        let (epoch, active) = fence.snapshot();
        if active {
            return;
        }
        let Some(promotion) = inner.shared.admission.reserve_promotion(path) else {
            return;
        };
        let _ = inner.enqueue_optional(move |optional| Work::Promote {
            context,
            epoch,
            optional,
            promotion,
        });
    }

    pub(crate) async fn shutdown(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        if rt::timeout(SHUTDOWN_TIMEOUT, inner.shutdown())
            .await
            .is_err()
        {
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
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::disk::open_disk;
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
}
