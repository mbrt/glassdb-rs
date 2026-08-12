//! Persistent encoded-body cache policy and path-lifetime coordination.

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend as backend;
use glassdb_concurr::rt;

use super::knowledge::{Knowledge, PresentSeed};
use super::path_lane::PathState;
use super::{Codec, ObjectKey, Revision};
use crate::cache_stats::CacheMetrics;
use crate::disk_cache::{EncodedBody, FenceContext, FenceGuard, PathFence, PersistentCache};
use crate::timeline::SequencePoint;

const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-path persistent-cache state owned by the path lane.
#[derive(Default)]
pub(super) struct PersistentPath {
    fence: PathFence,
}

/// The optional persistent tier behind the cached-store boundary.
#[derive(Clone)]
pub(super) struct PersistentBridge {
    cache: Option<PersistentCache>,
}

impl PersistentBridge {
    pub(super) fn new(cache: Option<PersistentCache>) -> Self {
        Self { cache }
    }

    pub(super) fn metrics(&self) -> Option<Arc<CacheMetrics>> {
        self.cache.as_ref().map(PersistentCache::metrics)
    }

    pub(super) fn is_configured(&self) -> bool {
        self.cache.is_some()
    }

    #[cfg(test)]
    pub(super) fn is_enabled(&self) -> bool {
        self.cache.as_ref().is_some_and(PersistentCache::is_enabled)
    }

    /// Drains and syncs the persistent tier when one is configured.
    pub(super) async fn shutdown(&self) {
        if let Some(cache) = &self.cache {
            cache.shutdown().await;
        }
    }

    /// Records a present-value hit for the persistent admission filter.
    pub(super) fn record_present_hit(&self, path: &Arc<str>, state: &Arc<PathState>) {
        let Some(cache) = &self.cache else {
            return;
        };
        cache.record_present_hit(path, PathLease::shared(state));
    }

    /// Restores and decodes one usable persistent candidate into L1 knowledge.
    pub(super) async fn load<C: Codec>(
        &self,
        knowledge: &Knowledge,
        key: &ObjectKey,
        state: &Arc<PathState>,
    ) -> Option<PresentSeed> {
        let cache = self.cache.as_ref()?;
        let lease = PathLease::shared(state);
        if lease.fence().is_active() || !cache.is_enabled() {
            return None;
        }
        let encoded = match rt::timeout(LOOKUP_TIMEOUT, cache.lookup(key.encoded().clone())).await {
            Ok(encoded) => encoded?,
            Err(_) => {
                cache.disable_slow_lookup();
                return None;
            }
        };
        self.decode_candidate::<C>(knowledge, key, cache, lease, encoded)
    }

    /// Starts a path-changing L2 transition before its corresponding L1 change.
    pub(super) fn begin_change(&self, state: &Arc<PathState>) -> PersistentChange {
        let active = self.cache.as_ref().and_then(|cache| {
            cache
                .begin_fence(PathLease::shared(state))
                .map(|fence| ActiveChange {
                    cache: cache.clone(),
                    fence,
                })
        });
        PersistentChange { active }
    }

    fn decode_candidate<C: Codec>(
        &self,
        knowledge: &Knowledge,
        key: &ObjectKey,
        cache: &PersistentCache,
        lease: Arc<PathLease>,
        encoded: EncodedBody,
    ) -> Option<PresentSeed> {
        let token = match String::from_utf8(encoded.revision) {
            Ok(token) => token,
            Err(error) => {
                tracing::warn!(path = %key.as_str(), %error, "discarding invalid persistent-cache revision");
                cache.reject_corrupt_candidate(key.encoded().clone(), lease);
                return None;
            }
        };
        let decoded = match C::decode(key.object_path(), &encoded.body) {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::warn!(path = %key.as_str(), %error, "discarding undecodable persistent-cache body");
                cache.reject_corrupt_candidate(key.encoded().clone(), lease);
                return None;
            }
        };
        let size = C::size(&decoded);
        let value = Arc::new(decoded);
        let revision = Revision(backend::Version::new(token));
        let seed = knowledge.install_persistent::<C>(
            key.as_str(),
            value,
            size,
            revision,
            encoded.current_after,
        );
        cache.record_present_hit(key.encoded(), lease);
        Some(seed)
    }
}

struct PathLease {
    state: Arc<PathState>,
}

impl PathLease {
    fn shared(state: &Arc<PathState>) -> Arc<Self> {
        Arc::new(Self {
            state: state.clone(),
        })
    }
}

impl FenceContext for PathLease {
    fn fence(&self) -> &PathFence {
        &self.state.persistent().fence
    }
}

struct ActiveChange {
    cache: PersistentCache,
    fence: FenceGuard,
}

/// An L2 fence established before a path-changing L1 transition.
#[must_use]
pub(super) struct PersistentChange {
    active: Option<ActiveChange>,
}

impl PersistentChange {
    /// Publishes a backend-read body under this path fence.
    pub(super) fn replace(
        mut self,
        path: Arc<str>,
        revision: &Revision,
        body: Vec<u8>,
        current_after: SequencePoint,
    ) {
        let Some(active) = self.active.take() else {
            return;
        };
        active.cache.replace(
            path,
            revision.serialize().as_bytes().to_vec(),
            body,
            current_after,
            active.fence,
        );
    }

    /// Clears every persistent candidate for the changed path.
    pub(super) fn invalidate(mut self, path: Arc<str>) {
        let Some(active) = self.active.take() else {
            return;
        };
        active.cache.invalidate(path, active.fence);
    }
}
