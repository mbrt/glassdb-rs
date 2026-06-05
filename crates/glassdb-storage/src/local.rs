//! The local, staleness-aware cache of values and metadata. Ported from the Go
//! `internal/storage/local.go`.

use std::sync::Arc;
use std::time::Duration;

// Use tokio's `Instant` so cache staleness honors `tokio::time::pause`/`advance`
// in tests; outside a paused runtime it behaves like a real monotonic clock.
use tokio::time::Instant;

use glassdb_backend::Metadata;
use glassdb_data::TxId;

use crate::cache::{Cache, Weighable};
use crate::locker::last_writer_from_tags;
use crate::version::Version;

/// The maximum staleness; effectively "any cached value is acceptable".
pub const MAX_STALENESS: Duration = Duration::MAX;

#[derive(Clone)]
struct CacheValue {
    value: Vec<u8>,
    deleted: bool,
    /// Marks the value as outdated for sure. When false, the status is unknown.
    outdated: bool,
    version: Version,
    updated: Instant,
}

#[derive(Clone)]
struct CacheMeta {
    /// Shared so cache reads and write-throughs hand out the metadata without
    /// deep-copying the tag map on every backend operation.
    meta: Arc<Metadata>,
    /// Last-writer decoded from `meta.tags`, cached to avoid re-parsing.
    writer: TxId,
    updated: Instant,
}

#[derive(Clone, Default)]
struct CacheEntry {
    v: Option<CacheValue>,
    m: Option<CacheMeta>,
}

impl Weighable for CacheEntry {
    fn size_b(&self) -> usize {
        let mut res = 0;
        if let Some(v) = &self.v {
            res += v.value.len() + v.version.writer.len();
        }
        if let Some(m) = &self.m {
            res += m.meta.tags.len() * 16;
        }
        res
    }
}

impl CacheEntry {
    fn is_meta_outdated(&self) -> bool {
        let Some(v) = &self.v else {
            return false;
        };
        let Some(m) = &self.m else {
            return false;
        };
        if v.version.b.is_null() {
            return false;
        }
        if v.version.writer == m.writer {
            return false;
        }
        // Writers differ: if the value was updated last, the metadata is
        // definitely outdated.
        v.updated > m.updated
    }

    fn is_value_outdated(&self) -> bool {
        let Some(v) = &self.v else {
            return false;
        };
        if v.outdated {
            return true;
        }
        let Some(m) = &self.m else {
            return false;
        };
        if v.version.b.is_null() {
            return false;
        }
        if v.version.writer == m.writer {
            return false;
        }
        // Writers differ: if the metadata was updated last, the value is
        // definitely outdated.
        m.updated > v.updated
    }
}

/// The result of reading a value from the local cache.
#[derive(Debug, Clone)]
pub struct LocalRead {
    pub value: Vec<u8>,
    pub version: Version,
    pub deleted: bool,
    /// True if the value is certainly outdated.
    pub outdated: bool,
}

/// Cached metadata along with its freshness status.
#[derive(Debug, Clone)]
pub struct LocalMetadata {
    pub m: Arc<Metadata>,
    /// True if the metadata is certainly outdated.
    pub outdated: bool,
}

/// A local in-memory cache for storage values and metadata with staleness
/// tracking.
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
        let e = self.cache.get(key)?;
        let v = e.v.as_ref()?;
        if is_stale(v.updated, max_stale) {
            return None;
        }
        Some(LocalRead {
            value: v.value.clone(),
            version: v.version.clone(),
            deleted: v.deleted,
            outdated: e.is_value_outdated(),
        })
    }

    /// Reads the cached metadata, if present and not staler than `max_stale`.
    pub fn get_meta(&self, key: &str, max_stale: Duration) -> Option<LocalMetadata> {
        let e = self.cache.get(key)?;
        let m = e.m.as_ref()?;
        if is_stale(m.updated, max_stale) {
            return None;
        }
        Some(LocalMetadata {
            m: m.meta.clone(),
            outdated: e.is_meta_outdated(),
        })
    }

    /// Stores both the value and its metadata atomically.
    pub fn write_with_meta(&self, key: &str, value: Vec<u8>, meta: Arc<Metadata>) {
        let updated = Instant::now();
        let writer = last_writer_from_tags(&meta.tags);
        let entry = CacheEntry {
            v: Some(CacheValue {
                value,
                deleted: false,
                outdated: false,
                version: Version {
                    b: meta.version.clone(),
                    writer: writer.clone(),
                },
                updated,
            }),
            m: Some(CacheMeta {
                meta,
                writer,
                updated,
            }),
        };
        self.cache.set(key, entry);
    }

    /// Updates only the value for `key`.
    pub fn write(&self, key: &str, value: Vec<u8>, v: Version) {
        let new_value = CacheValue {
            value,
            deleted: false,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache.update(key, move |old| match old {
            None => Some(CacheEntry {
                v: Some(new_value),
                m: None,
            }),
            Some(mut entry) => {
                entry.v = Some(new_value);
                Some(entry)
            }
        });
    }

    /// Updates only the metadata for `key`.
    pub fn set_meta(&self, key: &str, meta: Arc<Metadata>) {
        let writer = last_writer_from_tags(&meta.tags);
        let new_meta = CacheMeta {
            meta,
            writer,
            updated: Instant::now(),
        };
        self.cache.update(key, move |old| match old {
            None => Some(CacheEntry {
                v: None,
                m: Some(new_meta),
            }),
            Some(mut entry) => {
                entry.m = Some(new_meta);
                Some(entry)
            }
        });
    }

    /// Marks the value at `key` as outdated, only if it is at version `v`.
    pub fn mark_value_outdated(&self, key: &str, v: Version) {
        self.cache.update(key, move |old| {
            let mut entry = old?;
            if let Some(val) = &entry.v {
                if val.version.equal_contents(&v) {
                    let mut newval = val.clone();
                    newval.outdated = true;
                    entry.v = Some(newval);
                }
            }
            Some(entry)
        });
    }

    /// Marks `key` as deleted at version `v`.
    pub fn mark_deleted(&self, key: &str, v: Version) {
        let new_value = CacheValue {
            value: Vec::new(),
            deleted: true,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache.update(key, move |old| match old {
            None => Some(CacheEntry {
                v: Some(new_value),
                m: None,
            }),
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
