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
mod change;
mod disk;
mod file_media;
mod format;
mod media;
#[cfg(all(feature = "sim", sim))]
pub(crate) mod sim_harness;
#[cfg(any(test, feature = "sim"))]
pub(crate) mod sim_media;
mod worker;

pub(crate) use change::{PathChangeOwner, PathChanges, PendingChange};
use file_media::FileMedia;
use format::{CacheGeometry, PRODUCTION_GEOMETRY};
use media::CacheMedia;
use worker::{CacheInner, WatermarkAdvance, Work};

const CACHE_FILE: &str = "l2.cache";

/// Configuration for the optional persistent encoded-body cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentCacheConfig {
    /// Directory containing the cache's `l2.cache` file.
    pub directory: PathBuf,
    /// Maximum file size, rounded down to the cache block size. Production
    /// caches require at least 131 MiB; 512 MiB or more is recommended.
    pub capacity_bytes: u64,
    /// Time limit for the whole open, including the wait for the file lock.
    ///
    /// While another owner holds the lock, the open retries. When the limit
    /// passes, the cache is disabled. A live owner of the same directory
    /// therefore delays the open by the full limit.
    pub open_timeout: Duration,
    /// Time limit for a shutdown. When the limit passes, the cache is
    /// disabled and the worker is aborted without a clean-shutdown record.
    pub shutdown_timeout: Duration,
}

impl PersistentCacheConfig {
    /// Default value for [`Self::open_timeout`].
    pub const DEFAULT_OPEN_TIMEOUT: Duration = Duration::from_secs(20);
    /// Default value for [`Self::shutdown_timeout`].
    pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
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
    /// The start of the state's currentness interval, when this or an earlier
    /// open observed it.
    pub(crate) start: Option<SequencePoint>,
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

    pub(crate) fn begin_change(&self, owner: Arc<dyn PathChangeOwner>) -> Option<PendingChange> {
        let inner = self.inner.as_ref()?;
        if !inner.shared.enabled.load(Ordering::Acquire) {
            return None;
        }
        let Some(change) = inner.shared.change_limit.begin(owner) else {
            inner.disable_message("persistent-cache pending-change capacity exhausted");
            return None;
        };
        Some(change)
    }

    pub(crate) fn disable_slow_lookup(&self) {
        if let Some(inner) = &self.inner {
            inner.disable_message("persistent-cache lookup timed out");
        }
    }

    pub(crate) fn reject_corrupt_candidate(&self, path: Arc<str>, owner: Arc<dyn PathChangeOwner>) {
        self.metrics.l2_error();
        if let Some(change) = self.begin_change(owner) {
            self.invalidate(path, change);
        }
    }

    pub(crate) fn replace(
        &self,
        path: Arc<str>,
        revision: Vec<u8>,
        body: Vec<u8>,
        current_after: SequencePoint,
        start: Option<SequencePoint>,
        change: PendingChange,
    ) {
        let Some(inner) = &self.inner else {
            return;
        };
        if u32::try_from(path.len()).is_err() {
            self.invalidate(path, change);
            return;
        }
        let format = &inner.shared.disk.format;
        let size = match format.record_bytes(revision.len(), body.len()) {
            Some(size) if size <= format.maximum_record_bytes() => size,
            _ => {
                self.invalidate(path, change);
                return;
            }
        };
        let Some(payload) = inner.shared.admission.reserve_payload(size) else {
            self.invalidate(path, change);
            return;
        };
        inner.enqueue_required(Work::Replace {
            path,
            revision,
            body,
            current_after,
            start,
            change,
            payload,
        });
    }

    pub(crate) fn invalidate(&self, path: Arc<str>, change: PendingChange) {
        let Some(inner) = &self.inner else {
            return;
        };
        if u32::try_from(path.len()).is_err() {
            return;
        }
        inner.enqueue_required(Work::Invalidate { path, change });
    }

    pub(crate) fn record_present_hit(&self, path: &Arc<str>, owner: Arc<dyn PathChangeOwner>) {
        let Some(inner) = &self.inner else {
            return;
        };
        let path_changes = owner.path_changes();
        if !inner.shared.enabled.load(Ordering::Acquire)
            || path_changes.is_pending()
            || u32::try_from(path.len()).is_err()
        {
            return;
        }
        let fingerprint = inner.shared.disk.path_fingerprint(path);
        if !inner.shared.admission.observe_hit(fingerprint) {
            return;
        }
        let (latest, pending) = path_changes.snapshot();
        if pending {
            return;
        }
        let Some(promotion) = inner.shared.admission.reserve_promotion(path) else {
            return;
        };
        let _ = inner.enqueue_optional(move |optional| Work::Promote {
            owner,
            latest,
            optional,
            promotion,
        });
    }

    /// Queues a newer watermark for the persistent copy of one state, given by
    /// its path and revision. The worker writes it only into the index slot of
    /// that exact record, and only when the latest path change at this call
    /// has ended and no later one started. Returns whether the work was
    /// queued; full queues drop it.
    pub(crate) fn advance_watermark(
        &self,
        path: Arc<str>,
        owner: Arc<dyn PathChangeOwner>,
        revision: Vec<u8>,
        current_after: SequencePoint,
    ) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        if !inner.shared.enabled.load(Ordering::Acquire) || u32::try_from(path.len()).is_err() {
            return false;
        }
        // A change that is pending now either has its work ahead of this one,
        // and ends before the worker takes it, or the worker skips this work.
        let (latest, _) = owner.path_changes().snapshot();
        inner.enqueue_optional(move |optional| Work::Advance {
            path,
            owner,
            latest,
            revision,
            current_after,
            optional,
        })
    }

    /// Queues the newer watermarks of a clean shutdown as one work item, under
    /// the same rules as [`Self::advance_watermark`]. Returns whether it was
    /// queued.
    pub(crate) fn advance_watermarks(
        &self,
        advances: impl IntoIterator<Item = (Arc<str>, Arc<dyn PathChangeOwner>, Vec<u8>, SequencePoint)>,
    ) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        let advances = advances
            .into_iter()
            .filter(|(path, ..)| u32::try_from(path.len()).is_ok())
            .map(|(path, owner, revision, current_after)| {
                let (latest, _) = owner.path_changes().snapshot();
                WatermarkAdvance {
                    path,
                    owner,
                    latest,
                    revision,
                    current_after,
                }
            })
            .collect::<Vec<_>>();
        if advances.is_empty() {
            return true;
        }
        inner.enqueue_unreserved(Work::AdvanceBatch(advances))
    }

    pub(crate) async fn shutdown(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        if rt::timeout(inner.shutdown_timeout, inner.shutdown())
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
        let open_timeout = config.open_timeout;
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

        match rt::timeout(open_timeout, result).await {
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
    use std::io;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;

    use super::disk::{Slot, open_disk};
    use super::format::COMPACT_GEOMETRY;
    use super::media::CacheFile;
    use super::sim_media::{MediaFaultProfile, SimMedia};
    use super::*;
    use tempfile::TempDir;

    const TEST_CAPACITY: u64 = 2 * 1024 * 1024;
    // Tests that reopen the directory must not depend on disk speed: a timed-out
    // shutdown drops the clean-shutdown record and keeps the lock until the
    // blocked I/O returns.
    const TEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3600);
    const RECORD_CONTENT_OFFSET: u64 = 56;

    fn id(byte: u8) -> DatabaseId {
        DatabaseId::from_bytes([byte; 16])
    }

    async fn open(dir: &TempDir, database_id: DatabaseId) -> PersistentCache {
        open_result(dir, database_id).await.cache
    }

    async fn open_result(dir: &TempDir, database_id: DatabaseId) -> OpenedPersistentCache {
        open_result_within(
            dir,
            database_id,
            PersistentCacheConfig::DEFAULT_OPEN_TIMEOUT,
        )
        .await
    }

    async fn open_result_within(
        dir: &TempDir,
        database_id: DatabaseId,
        open_timeout: Duration,
    ) -> OpenedPersistentCache {
        PersistentCache::open_with_geometry(
            PersistentCacheConfig {
                open_timeout,
                ..config(dir)
            },
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
            open_timeout: PersistentCacheConfig::DEFAULT_OPEN_TIMEOUT,
            shutdown_timeout: TEST_SHUTDOWN_TIMEOUT,
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
        publish_with_start(cache, path, revision, body, current_after, None);
    }

    fn publish_with_start(
        cache: &PersistentCache,
        path: &str,
        revision: &[u8],
        body: &[u8],
        current_after: SequencePoint,
        start: Option<SequencePoint>,
    ) {
        let path_changes = Arc::new(PathChanges::default());
        let change = cache.begin_change(path_changes).unwrap();
        cache.replace(
            Arc::from(path),
            revision.to_vec(),
            body.to_vec(),
            current_after,
            start,
            change,
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
            0x47, 0x4c, 0x32, 0x54, 0x45, 0x53, 0x54, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x0f, 0x94, 0x41, 0x63, 0x33, 0xcf, 0x8b, 0xd4, 0xb1, 0x34, 0x49, 0xad,
            0xc3, 0x54, 0xf6, 0x31, 0x52, 0x28, 0xc4, 0x95, 0x68, 0x04, 0xa1, 0x32, 0x2a, 0xac,
            0x2d, 0x9b, 0x6b, 0xf3, 0x0c, 0xc1, 0xf0, 0x79, 0x19, 0x17, 0x86, 0xc8, 0x89, 0xf5,
            0x3c, 0x7b, 0xbf, 0x60, 0x27, 0x41, 0x02, 0x1b, 0x61, 0x99, 0x70, 0x45, 0x4b, 0xa4,
            0x32, 0xc0, 0x58, 0xa8, 0x27, 0xa0, 0xb4, 0xd6, 0xb2, 0x0f,
        ];
        const SLOT: [u8; 56] = [
            0x0e, 0xbe, 0xe6, 0x9f, 0x3e, 0x56, 0x64, 0x1a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0xb0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc4, 0xd7, 0x26, 0x2c, 0x18, 0xf7, 0x80, 0x0d,
        ];
        const MARKER_PREFIX: [u8; 48] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x30, 0x29, 0xd6, 0x5f, 0x03, 0x34, 0x57, 0xdd, 0x41, 0xa5, 0x45, 0xa2,
            0x61, 0xd5, 0x63, 0xb6, 0x27, 0x9c, 0x70, 0x3e, 0x58, 0x8a, 0xdb, 0xa1, 0xa4, 0xa5,
            0x5a, 0xca, 0x70, 0xf9, 0x89, 0x00,
        ];
        const RECORD_PREFIX: [u8; 62] = [
            0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x66, 0x56, 0x72, 0x68,
            0x58, 0xbf, 0xa5, 0x55, 0x2e, 0xf6, 0x6b, 0xf0, 0x34, 0xa0, 0x18, 0xd9, 0x88, 0x9c,
            0x4f, 0xbc, 0xaa, 0x03, 0x10, 0x48, 0xaf, 0x49, 0xaf, 0xd1, 0x6f, 0x53, 0xd6, 0x39,
            0x72, 0x31, 0x62, 0x6f, 0x64, 0x79,
        ];
        const HEADER_OFFSET: usize = 0;
        const MARKER_OFFSET: usize = 4 * 1024;
        const SLOT_OFFSET: usize = 32 * 1024;
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
            .append("db/object", b"r1", b"body", point(17), Some(point(19)))
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
        assert_eq!(last_sequence_point, Some(point(19)));
        let record = disk.lookup("db/object").await.unwrap().unwrap();
        assert_eq!(record.revision, b"r1");
        assert_eq!(record.body, b"body");
        assert_eq!(record.current_after, point(17));
        assert_eq!(record.start, Some(point(19)));
        let resumed = writer
            .append("db/next", b"r2", b"next", point(20), None)
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
    async fn record_keeps_its_start() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_with_start(
            &cache,
            "db/observed",
            b"r1",
            b"a",
            point(17),
            Some(point(19)),
        );
        publish_at(&cache, "db/unknown", b"r2", b"b", point(18));

        let observed = cache.lookup(Arc::from("db/observed")).await.unwrap();
        assert_eq!(observed.current_after, point(17));
        assert_eq!(observed.start, Some(point(19)));
        let unknown = cache.lookup(Arc::from("db/unknown")).await.unwrap();
        assert_eq!(unknown.start, None);
        cache.shutdown().await;
    }

    fn advance(cache: &PersistentCache, path: &str, revision: &[u8], current_after: SequencePoint) {
        assert!(cache.advance_watermark(
            Arc::from(path),
            Arc::new(PathChanges::default()),
            revision.to_vec(),
            current_after,
        ));
    }

    #[tokio::test]
    async fn advance_updates_the_slot_watermark() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_with_start(&cache, "db/object", b"r1", b"a", point(8), Some(point(10)));
        advance(&cache, "db/object", b"r1", point(30));

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.body, b"a");
        assert_eq!(record.current_after, point(30));
        assert_eq!(record.start, Some(point(10)));

        // An older watermark never lowers the slot.
        advance(&cache, "db/object", b"r1", point(20));
        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.current_after, point(30));
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_advances_need_no_optional_reservation() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_with_start(&cache, "db/object", b"r1", b"a", point(8), Some(point(10)));
        let inner = cache.inner.as_ref().unwrap();
        // Hold every optional reservation, as a large shutdown flush of single
        // advances would.
        let held =
            std::iter::from_fn(|| inner.shared.admission.reserve_optional()).collect::<Vec<_>>();
        assert!(!cache.advance_watermark(
            Arc::from("db/object"),
            Arc::new(PathChanges::default()),
            b"r1".to_vec(),
            point(30),
        ));
        assert!(cache.advance_watermarks([(
            Arc::<str>::from("db/object"),
            Arc::new(PathChanges::default()) as Arc<dyn PathChangeOwner>,
            b"r1".to_vec(),
            point(30),
        )]));
        drop(held);

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.current_after, point(30));
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn advance_for_another_revision_is_ignored() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_with_start(&cache, "db/object", b"r2", b"a", point(8), Some(point(10)));
        advance(&cache, "db/object", b"r1", point(30));
        advance(&cache, "db/other", b"r2", point(30));

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.revision, b"r2");
        assert_eq!(record.current_after, point(8));
        assert!(cache.lookup(Arc::from("db/other")).await.is_none());
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn advance_during_a_path_change_is_ignored() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_at(&cache, "db/object", b"r1", b"a", point(8));
        let path = Arc::<str>::from("db/object");

        // A change starts after the advance is queued.
        let owner = Arc::new(PathChanges::default());
        assert!(cache.advance_watermark(path.clone(), owner.clone(), b"r1".to_vec(), point(30)));
        let later = cache.begin_change(owner.clone()).unwrap();
        assert_eq!(
            cache.lookup(path.clone()).await.unwrap().current_after,
            point(8)
        );
        drop(later);

        // A change is still pending when the worker takes the advance.
        let owner = Arc::new(PathChanges::default());
        let pending = cache.begin_change(owner.clone()).unwrap();
        assert!(cache.advance_watermark(path.clone(), owner.clone(), b"r1".to_vec(), point(40)));
        assert_eq!(
            cache.lookup(path.clone()).await.unwrap().current_after,
            point(8)
        );
        drop(pending);
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn advance_after_a_queued_replace_of_the_same_state_applies() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let path = Arc::<str>::from("db/object");
        let owner = Arc::new(PathChanges::default());
        let change = cache.begin_change(owner.clone()).unwrap();
        cache.replace(
            path.clone(),
            b"r1".to_vec(),
            b"a".to_vec(),
            point(8),
            None,
            change,
        );
        assert!(cache.advance_watermark(path.clone(), owner, b"r1".to_vec(), point(30)));
        assert_eq!(cache.lookup(path).await.unwrap().current_after, point(30));
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn corrupt_slot_watermark_is_a_miss() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        publish_at(&cache, "db/object", b"r1", b"a", point(8));
        advance(&cache, "db/object", b"r1", point(30));
        assert_eq!(
            cache
                .lookup(Arc::from("db/object"))
                .await
                .unwrap()
                .current_after,
            point(30)
        );

        let disk = &cache.inner.as_ref().unwrap().shared.disk;
        let slot = disk.current_slot("db/object").await.unwrap().unwrap();
        let encoded = disk.format.encode_slot(slot);
        let bucket = slot.fingerprint % disk.format.bucket_count();
        let bucket_offset = disk.format.metadata_bytes() + bucket * disk.format.block_bytes();
        let mut bytes = vec![0; disk.format.block_bytes() as usize];
        disk.file
            .read_exact_at(&mut bytes, bucket_offset)
            .await
            .unwrap();
        let slot_offset = bucket_offset
            + bytes
                .windows(encoded.len())
                .position(|window| window == encoded)
                .unwrap() as u64;
        // Claim a larger watermark than the advance wrote.
        let forged = Slot {
            current_after: point(1000),
            ..slot
        };
        disk.file
            .write_all_at(&disk.format.encode_slot(forged), slot_offset)
            .await
            .unwrap();

        assert!(cache.lookup(Arc::from("db/object")).await.is_none());
        advance(&cache, "db/object", b"r1", point(2000));
        assert!(cache.lookup(Arc::from("db/object")).await.is_none());
        cache.shutdown().await;
    }

    #[tokio::test]
    async fn reopen_maximum_covers_an_advanced_watermark() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let cache = open(&dir, database_id).await;
        publish_with_start(&cache, "db/object", b"r1", b"a", point(8), Some(point(10)));
        publish_at(&cache, "db/high", b"r2", b"b", point(34));
        advance(&cache, "db/object", b"r1", point(50));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, database_id).await;
        assert_eq!(opened.last_sequence_point, Some(point(50)));
        let record = opened.cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.current_after, point(50));
        assert_eq!(record.start, Some(point(10)));
        opened.cache.shutdown().await;
    }

    #[tokio::test]
    async fn reopen_maximum_covers_a_start_after_the_watermark() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let cache = open(&dir, database_id).await;
        publish_with_start(
            &cache,
            "db/observed",
            b"r1",
            b"a",
            point(8),
            Some(point(40)),
        );
        publish_at(&cache, "db/high", b"r2", b"b", point(34));
        cache.shutdown().await;
        drop(cache);

        let opened = open_result(&dir, database_id).await;
        assert_eq!(opened.last_sequence_point, Some(point(40)));
        opened.cache.shutdown().await;
    }

    #[tokio::test]
    async fn concurrent_owner_is_disabled_without_disturbing_the_owner() {
        let dir = TempDir::new().unwrap();
        let database_id = id(1);
        let owner = open(&dir, database_id).await;
        let contender = open_result_within(&dir, database_id, Duration::from_millis(200))
            .await
            .cache;

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

    /// Media whose `sync_data` keeps its file alive after the caller is
    /// dropped while a gate is closed. It models a blocking system call that an
    /// aborted worker cannot interrupt.
    struct StallableMedia {
        inner: SimMedia,
        open: tokio::sync::watch::Receiver<bool>,
        blocked: Arc<tokio::sync::Notify>,
    }

    struct StallableFile {
        inner: Arc<dyn CacheFile>,
        open: tokio::sync::watch::Receiver<bool>,
    }

    #[async_trait::async_trait]
    impl CacheMedia for StallableMedia {
        async fn open_exclusive(&self, directory: &Path) -> io::Result<Arc<dyn CacheFile>> {
            let inner = match self.inner.open_exclusive(directory).await {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.blocked.notify_one();
                    return Err(error);
                }
                result => result?,
            };
            Ok(Arc::new(StallableFile {
                inner,
                open: self.open.clone(),
            }))
        }
    }

    #[async_trait::async_trait]
    impl CacheFile for StallableFile {
        async fn len(&self) -> io::Result<u64> {
            self.inner.len().await
        }

        async fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len).await
        }

        async fn allocate(&self, len: u64) -> io::Result<()> {
            self.inner.allocate(len).await
        }

        async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            self.inner.read_exact_at(bytes, offset).await
        }

        async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
            self.inner.write_all_at(bytes, offset).await
        }

        async fn sync_data(&self) -> io::Result<()> {
            let inner = self.inner.clone();
            let mut open = self.open.clone();
            rt::spawn(async move {
                let result = inner.sync_data().await;
                let _ = open.wait_for(|is_open| *is_open).await;
                result
            })
            .await
            .expect("detached sync task panicked")
        }

        async fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all().await
        }
    }

    #[tokio::test]
    async fn open_waits_for_an_aborted_worker_to_release_the_lock() {
        let dir = TempDir::new().unwrap();
        let (gate, open) = tokio::sync::watch::channel(true);
        let blocked = Arc::new(tokio::sync::Notify::new());
        let media = Arc::new(StallableMedia {
            inner: SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1),
            open,
            blocked: blocked.clone(),
        });
        let open_cache = |config| {
            PersistentCache::open_with_geometry(
                config,
                "db",
                id(1),
                COMPACT_GEOMETRY,
                Arc::new(CacheMetrics::new()),
                media.clone(),
            )
        };
        let cache = open_cache(PersistentCacheConfig {
            shutdown_timeout: Duration::from_millis(100),
            ..config(&dir)
        })
        .await
        .cache;
        publish_at(&cache, "db/object", b"r1", b"body", point(17));

        // The sync of the clean shutdown stalls, so the shutdown times out and
        // aborts the worker. The detached sync still holds the file and its lock.
        gate.send(false).unwrap();
        cache.shutdown().await;
        assert!(!cache.is_enabled());
        drop(cache);

        // The detached sync returns only after an open found the lock held.
        let release = async {
            blocked.notified().await;
            gate.send(true).unwrap();
        };
        let (opened, ()) = tokio::join!(open_cache(config(&dir)), release);

        assert!(opened.cache.is_enabled());
        opened.cache.shutdown().await;
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
    async fn pending_change_retains_its_owner_until_release() {
        struct TestOwner {
            path_changes: PathChanges,
            dropped: Arc<AtomicBool>,
        }

        impl PathChangeOwner for TestOwner {
            fn path_changes(&self) -> &PathChanges {
                &self.path_changes
            }
        }

        impl Drop for TestOwner {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = Arc::new(TestOwner {
            path_changes: PathChanges::default(),
            dropped: dropped.clone(),
        });
        let change = cache.begin_change(owner.clone()).unwrap();

        drop(owner);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(change);
        assert!(dropped.load(Ordering::SeqCst));

        cache.shutdown().await;
    }

    #[tokio::test]
    async fn newer_path_change_cancels_an_older_admission() {
        let dir = TempDir::new().unwrap();
        let cache = open(&dir, id(1)).await;
        let path_changes = Arc::new(PathChanges::default());
        let older = cache.begin_change(path_changes.clone()).unwrap();
        let newer = cache.begin_change(path_changes.clone()).unwrap();

        cache.replace(
            Arc::from("db/object"),
            b"r1".to_vec(),
            b"old".to_vec(),
            point(1),
            None,
            older,
        );
        cache.replace(
            Arc::from("db/object"),
            b"r2".to_vec(),
            b"new".to_vec(),
            point(2),
            None,
            newer,
        );

        let record = cache.lookup(Arc::from("db/object")).await.unwrap();
        assert_eq!(record.revision, b"r2");
        assert_eq!(record.body, b"new");
        assert!(!path_changes.is_pending());
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
            .append("db/object", b"r1", b"body", point(1), None)
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
            .append("db/new", b"r2", b"new", point(2), None)
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
