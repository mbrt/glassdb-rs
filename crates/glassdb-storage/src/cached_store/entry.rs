//! The entries of the shared decoded cache: the latest state of a path, and
//! its earlier states (ADR-103).

use std::any::Any;
use std::sync::Arc;

use crate::cache::{Cache, EvictionHook, Slot, Weighable};
use crate::error::StorageError;

use super::{Codec, Evidence, Revision};

pub(super) type ErasedValue = Arc<dyn Any + Send + Sync>;

/// Receives each present state that the decoded cache evicts for capacity,
/// so that the persistent cache can keep its newest watermark (ADR-097).
pub(super) trait EvictionSink: Send + Sync {
    fn evicted(&self, path: &str, revision: &Revision, evidence: &Evidence);
}

/// The latest known state of a path: either a present decoded value or a
/// confirmed absence. A missing object has no entry at all.
#[derive(Clone)]
pub(super) enum LatestState {
    Present {
        value: ErasedValue,
        size: usize,
        revision: Revision,
        evidence: Evidence,
    },
    Absent {
        evidence: Evidence,
    },
}

/// A present state of a path that a newer state replaced, kept with its
/// evidence. The evidence keeps growing: a CAS of this instance advances the
/// watermark of the state that it replaces.
#[derive(Clone)]
pub(super) struct EarlierState {
    pub(super) value: ErasedValue,
    pub(super) size: usize,
    pub(super) revision: Revision,
    pub(super) evidence: Evidence,
}

/// The earlier states of one path, newest first. A cache read clones the
/// entry, so the states are shared.
#[derive(Clone)]
pub(super) struct PathEarlierStates(pub(super) Arc<[EarlierState]>);

/// One entry of the decoded cache. A [`Slot::Latest`] entry holds a
/// [`CacheEntry::Latest`], and a [`Slot::Earlier`] entry holds a
/// [`CacheEntry::Earlier`].
#[derive(Clone)]
pub(super) enum CacheEntry {
    Latest(LatestState),
    Earlier(PathEarlierStates),
}

impl CacheEntry {
    pub(super) fn into_latest(self) -> Option<LatestState> {
        match self {
            CacheEntry::Latest(state) => Some(state),
            CacheEntry::Earlier(_) => None,
        }
    }

    pub(super) fn into_earlier(self) -> Option<PathEarlierStates> {
        match self {
            CacheEntry::Earlier(states) => Some(states),
            CacheEntry::Latest(_) => None,
        }
    }
}

impl Weighable for CacheEntry {
    fn size(&self) -> usize {
        // A present state weighs its decoded size plus the revision token; an
        // absent state costs a small fixed bookkeeping amount.
        const OVERHEAD: usize = std::mem::size_of::<CacheEntry>();
        const EARLIER_OVERHEAD: usize = std::mem::size_of::<EarlierState>();
        match self {
            CacheEntry::Latest(LatestState::Present { size, revision, .. }) => {
                size + revision.serialize().len() + OVERHEAD
            }
            CacheEntry::Latest(LatestState::Absent { .. }) => OVERHEAD,
            CacheEntry::Earlier(states) => {
                OVERHEAD
                    + states
                        .0
                        .iter()
                        .map(|state| {
                            state.size + state.revision.serialize().len() + EARLIER_OVERHEAD
                        })
                        .sum::<usize>()
            }
        }
    }
}

/// The shared decoded cache.
pub(super) type DecodedCache = Arc<Cache<CacheEntry>>;

/// Creates the decoded cache. When `sink` is set, the cache passes to it each
/// latest present state that it evicts for capacity.
pub(super) fn decoded_cache(max_size: usize, sink: Option<Arc<dyn EvictionSink>>) -> DecodedCache {
    let cache = match sink {
        Some(sink) => {
            let hook: EvictionHook<CacheEntry> = Box::new(move |slot, path, entry| {
                if slot == Slot::Latest
                    && let CacheEntry::Latest(LatestState::Present {
                        revision, evidence, ..
                    }) = entry
                {
                    sink.evicted(path, &revision, &evidence);
                }
            });
            Cache::with_eviction(max_size, hook)
        }
        None => Cache::new(max_size),
    };
    Arc::new(cache)
}

/// Downcasts a type-erased cached value to the codec's decoded type. A mismatch
/// means a path was used through the wrong typed store, which is an internal
/// error.
pub(super) fn downcast<C: Codec>(
    path: &str,
    value: ErasedValue,
) -> Result<Arc<C::Value>, StorageError> {
    value.downcast::<C::Value>().map_err(|_| {
        StorageError::other(format!(
            "cached object at {path} has a different decoded type than {}",
            C::name()
        ))
    })
}
