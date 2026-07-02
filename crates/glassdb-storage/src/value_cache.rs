//! The writer-keyed, staleness-aware value cache (ADR-023). A user key's value
//! lives in the transaction object of whichever transaction last committed it,
//! so a cached value is identified by its writer, not a backend object version.
//!
//! It is one of two facades over a single shared LRU ([`crate::SharedCache`]);
//! the other is the backend-version-keyed [`crate::ObjectCache`]. Both are built
//! from the same [`crate::SharedCache`], so a single `cache_size` bounds both.

use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::rt::Instant;

use crate::cache::Cache;
use crate::entry::{CacheEntry, SharedCache, ValueEntry};
use crate::version::Version;

/// The maximum staleness; effectively "any cached value is acceptable".
pub const MAX_STALENESS: Duration = Duration::MAX;

/// The result of reading a value from the value cache.
#[derive(Debug, Clone)]
pub struct ValueRead {
    pub value: Arc<[u8]>,
    pub version: Version,
    pub deleted: bool,
    /// True if the value is certainly outdated.
    pub outdated: bool,
}

/// An in-memory cache for user values with staleness tracking, keyed by the
/// writer that last committed the value.
#[derive(Clone)]
pub struct ValueCache {
    cache: Arc<Cache<CacheEntry>>,
}

impl ValueCache {
    /// Creates the value facade over `cache`.
    pub fn new(cache: &SharedCache) -> Self {
        ValueCache {
            cache: cache.handle(),
        }
    }

    /// Reads the cached value, if present and not staler than `max_stale`.
    pub fn read(&self, key: &str, max_stale: Duration) -> Option<ValueRead> {
        // `cache.get` already hands back an owned (cloned) entry, so move the
        // value and version out of it instead of cloning them a second time.
        let CacheEntry::Value(v) = self.cache.get(key)? else {
            return None;
        };
        if is_stale(v.updated, max_stale) {
            return None;
        }
        Some(ValueRead {
            value: v.value,
            version: v.version,
            deleted: v.deleted,
            outdated: v.outdated,
        })
    }

    /// Updates the value for `key`.
    pub fn write(&self, key: &str, value: Arc<[u8]>, v: Version) {
        let new_value = ValueEntry {
            value,
            deleted: false,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache
            .update(key, move |_| Some(CacheEntry::Value(new_value)));
    }

    /// Marks the value at `key` as outdated, only if it is at version `v`.
    pub fn mark_value_outdated(&self, key: &str, v: Version) {
        self.cache.update(key, move |old| match old {
            Some(CacheEntry::Value(mut val)) => {
                if val.version.equal_contents(&v) {
                    val.outdated = true;
                }
                Some(CacheEntry::Value(val))
            }
            other => other,
        });
    }

    /// Marks `key` as deleted at version `v`.
    pub fn mark_deleted(&self, key: &str, v: Version) {
        let new_value = ValueEntry {
            value: Arc::from(&[] as &[u8]),
            deleted: true,
            outdated: false,
            version: v,
            updated: Instant::now(),
        };
        self.cache
            .update(key, move |_| Some(CacheEntry::Value(new_value)));
    }

    /// Removes `key` entirely.
    pub fn delete(&self, key: &str) {
        self.cache.delete(key);
    }
}

fn is_stale(updated: Instant, max_staleness: Duration) -> bool {
    updated.elapsed() > max_staleness
}
