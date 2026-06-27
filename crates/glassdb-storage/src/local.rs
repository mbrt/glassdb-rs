//! The local, staleness-aware cache of values. Ported from the Go
//! `internal/storage/local.go`, slimmed to the v2 tagless layout (ADR-023): the
//! cache no longer tracks object metadata/tags, only values and their version.

use std::sync::Arc;
use std::time::Duration;

// Use the runtime `Instant` so cache staleness honors `tokio::time::pause`/
// `advance` in tests and the deterministic executor's virtual clock under
// `--cfg sim`; outside either it behaves like a real monotonic clock.
use glassdb_concurr::rt::Instant;

use crate::cache::{Cache, Weighable};
use crate::version::Version;

/// The maximum staleness; effectively "any cached value is acceptable".
pub const MAX_STALENESS: Duration = Duration::MAX;

#[derive(Clone)]
struct CacheValue {
    value: Arc<[u8]>,
    deleted: bool,
    /// Marks the value as outdated for sure. When false, the status is unknown.
    outdated: bool,
    version: Version,
    updated: Instant,
}

#[derive(Clone, Default)]
struct CacheEntry {
    v: Option<CacheValue>,
}

impl Weighable for CacheEntry {
    fn size_b(&self) -> usize {
        let mut res = 0;
        if let Some(v) = &self.v {
            res += v.value.len() + v.version.writer.as_bytes().len();
        }
        res
    }
}

/// The result of reading a value from the local cache.
#[derive(Debug, Clone)]
pub struct LocalRead {
    pub value: Arc<[u8]>,
    pub version: Version,
    pub deleted: bool,
    /// True if the value is certainly outdated.
    pub outdated: bool,
}

/// A local in-memory cache for storage values with staleness tracking.
#[derive(Clone)]
pub struct Local {
    cache: Arc<Cache<CacheEntry>>,
}

impl Local {
    /// Creates a local cache with the given maximum size in bytes.
    pub fn new(max_size_b: usize) -> Self {
        Local {
            cache: Arc::new(Cache::new(max_size_b)),
        }
    }

    /// Reads the cached value, if present and not staler than `max_stale`.
    pub fn read(&self, key: &str, max_stale: Duration) -> Option<LocalRead> {
        // `cache.get` already hands back an owned (cloned) entry, so move the
        // value and version out of it instead of cloning them a second time.
        let e = self.cache.get(key)?;
        let v = e.v?;
        if is_stale(v.updated, max_stale) {
            return None;
        }
        Some(LocalRead {
            value: v.value,
            version: v.version,
            deleted: v.deleted,
            outdated: v.outdated,
        })
    }

    /// Updates the value for `key`.
    pub fn write(&self, key: &str, value: Arc<[u8]>, v: Version) {
        let new_value = CacheValue {
            value,
            deleted: false,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache.update(key, move |old| match old {
            None => Some(CacheEntry { v: Some(new_value) }),
            Some(mut entry) => {
                entry.v = Some(new_value);
                Some(entry)
            }
        });
    }

    /// Marks the value at `key` as outdated, only if it is at version `v`.
    pub fn mark_value_outdated(&self, key: &str, v: Version) {
        self.cache.update(key, move |old| {
            let mut entry = old?;
            if let Some(val) = &entry.v
                && val.version.equal_contents(&v)
            {
                let mut newval = val.clone();
                newval.outdated = true;
                entry.v = Some(newval);
            }
            Some(entry)
        });
    }

    /// Marks `key` as deleted at version `v`.
    pub fn mark_deleted(&self, key: &str, v: Version) {
        let new_value = CacheValue {
            value: Arc::from(&[] as &[u8]),
            deleted: true,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache.update(key, move |old| match old {
            None => Some(CacheEntry { v: Some(new_value) }),
            Some(mut entry) => {
                entry.v = Some(new_value);
                Some(entry)
            }
        });
    }

    /// Removes `key` entirely.
    pub fn delete(&self, key: &str) {
        self.cache.delete(key);
    }
}

fn is_stale(updated: Instant, max_staleness: Duration) -> bool {
    updated.elapsed() > max_staleness
}
