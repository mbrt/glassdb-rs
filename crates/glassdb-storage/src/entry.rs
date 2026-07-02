//! The shared cache entry backing both cache facades. A single byte-weighted
//! LRU ([`crate::cache::Cache`]) is sized once (`cache_size`) and shared by two
//! facades that key disjoint path namespaces: the writer-keyed
//! [`crate::ValueCache`] and the backend-version-keyed [`crate::ObjectCache`].

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_concurr::rt::Instant;

use crate::cache::{Cache, Weighable};
use crate::version::Version;

/// The single LRU cache both facades are built from. Construct one, then hand it
/// to [`crate::ValueCache::new`] and [`crate::ObjectCache::new`] so a single
/// `cache_size` bounds both.
#[derive(Clone)]
pub struct SharedCache {
    inner: Arc<Cache<CacheEntry>>,
}

impl SharedCache {
    /// Creates a shared cache holding at most `max_size_b` bytes across both
    /// facades.
    pub fn new(max_size_b: usize) -> Self {
        SharedCache {
            inner: Arc::new(Cache::new(max_size_b)),
        }
    }

    pub(crate) fn handle(&self) -> Arc<Cache<CacheEntry>> {
        self.inner.clone()
    }
}

/// A cached user value, identified by the transaction that last committed it
/// (ADR-023): the value lives in that writer's immutable transaction object, so
/// a cached value is identified by its writer, not a backend object version.
#[derive(Clone)]
pub(crate) struct ValueEntry {
    pub value: Arc<[u8]>,
    pub deleted: bool,
    /// Marks the value as outdated for sure. When false, the status is unknown.
    pub outdated: bool,
    pub version: Version,
    pub updated: Instant,
}

/// A cached coordination object (shard, collection root, transaction log),
/// identified by its backend version (an opaque content-CAS token) so it can be
/// revalidated with a version-conditional read.
#[derive(Clone)]
pub(crate) struct ObjectEntry {
    pub bytes: Arc<[u8]>,
    pub version: backend::Version,
}

/// One entry in the shared LRU cache. The two facades key disjoint path
/// namespaces, so an entry is only ever read back as the variant that wrote it.
#[derive(Clone)]
pub(crate) enum CacheEntry {
    Value(ValueEntry),
    Object(ObjectEntry),
}

impl Weighable for CacheEntry {
    fn size_b(&self) -> usize {
        match self {
            CacheEntry::Value(v) => v.value.len() + v.version.writer.as_bytes().len(),
            CacheEntry::Object(o) => o.bytes.len() + o.version.token.len(),
        }
    }
}
