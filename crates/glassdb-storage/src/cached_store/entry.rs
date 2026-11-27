//! The entries of the shared decoded cache: the latest state of a path, and
//! its earlier states (ADR-103).

use std::any::Any;
use std::sync::Arc;

use glassdb_data::ObjectPath;

use crate::cache::{Cache, EvictionHook, Slot, Weighable};
use crate::error::StorageError;

use super::{Codec, Evidence, ObjectKey, Revision};

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
/// [`CacheEntry::Earlier`]. Only [`DecodedCache`] builds and reads entries, so
/// that each slot holds its own kind.
#[derive(Clone)]
enum CacheEntry {
    Latest(LatestState),
    Earlier(PathEarlierStates),
}

impl CacheEntry {
    fn into_latest(self) -> Option<LatestState> {
        match self {
            CacheEntry::Latest(state) => Some(state),
            CacheEntry::Earlier(_) => None,
        }
    }

    fn into_earlier(self) -> Option<PathEarlierStates> {
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

/// The shared decoded cache: the latest state and the earlier states of each
/// path, in one budget and one least-recently-used order (ADR-103).
#[derive(Clone)]
///
/// A lookup takes the path of an object, so that a hit formats no backend
/// name. A change takes its key, which the cache keeps.
pub(super) struct DecodedCache(Arc<Cache<ObjectKey, CacheEntry>>);

impl DecodedCache {
    /// Creates the decoded cache. When `sink` is set, the cache passes to it
    /// each latest present state that it evicts for capacity.
    pub(super) fn new(max_size: usize, sink: Option<Arc<dyn EvictionSink>>) -> Self {
        let cache = match sink {
            Some(sink) => {
                let hook: EvictionHook<ObjectKey, CacheEntry> =
                    Box::new(move |slot, key, entry| {
                        if slot == Slot::Latest
                            && let CacheEntry::Latest(LatestState::Present {
                                revision,
                                evidence,
                                ..
                            }) = entry
                        {
                            sink.evicted(key.as_str(), &revision, &evidence);
                        }
                    });
                Cache::with_eviction(max_size, hook)
            }
            None => Cache::new(max_size),
        };
        Self(Arc::new(cache))
    }

    /// Returns the latest state of `path`, and moves it to the
    /// most-recently-used position.
    pub(super) fn latest(&self, path: &ObjectPath) -> Option<LatestState> {
        self.0
            .get(Slot::Latest, path)
            .and_then(CacheEntry::into_latest)
    }

    /// Returns the earlier states of `path`, and moves them to the
    /// most-recently-used position.
    pub(super) fn earlier(&self, path: &ObjectPath) -> Option<PathEarlierStates> {
        self.0
            .get(Slot::Earlier, path)
            .and_then(CacheEntry::into_earlier)
    }

    /// Updates the latest state of `path` while holding the lock, and returns
    /// an auxiliary result. `None` removes the state.
    pub(super) fn update_latest<R>(
        &self,
        path: &ObjectKey,
        f: impl FnOnce(Option<LatestState>) -> (Option<LatestState>, R),
    ) -> R {
        self.0.update_with_result(Slot::Latest, path, |old| {
            let (state, result) = f(old.and_then(CacheEntry::into_latest));
            (state.map(CacheEntry::Latest), result)
        })
    }

    /// Updates the earlier states of `path` while holding the lock. `None`
    /// removes them.
    pub(super) fn update_earlier(
        &self,
        path: &ObjectKey,
        f: impl FnOnce(Option<PathEarlierStates>) -> Option<PathEarlierStates>,
    ) {
        self.0.update(Slot::Earlier, path, |old| {
            f(old.and_then(CacheEntry::into_earlier)).map(CacheEntry::Earlier)
        });
    }

    /// Updates the latest state and the earlier states of `path` in one step.
    /// The closure returns the new latest state, the new earlier states or
    /// `None` to leave them as they are, and an auxiliary result.
    pub(super) fn update_both<R>(
        &self,
        path: &ObjectKey,
        f: impl FnOnce(
            Option<LatestState>,
            Option<PathEarlierStates>,
        ) -> (LatestState, Option<PathEarlierStates>, R),
    ) -> R {
        self.0.update_both(path, |latest, earlier| {
            let (latest, earlier, result) = f(
                latest.and_then(CacheEntry::into_latest),
                earlier.and_then(CacheEntry::into_earlier),
            );
            (
                Some(CacheEntry::Latest(latest)),
                earlier.map(CacheEntry::Earlier),
                result,
            )
        })
    }

    /// Removes the latest state of `path`.
    pub(super) fn delete_latest(&self, path: &ObjectPath) {
        self.0.delete(Slot::Latest, path);
    }

    /// Removes the earlier states of `path`.
    pub(super) fn delete_earlier(&self, path: &ObjectPath) {
        self.0.delete(Slot::Earlier, path);
    }

    /// Returns a copy of the latest state of every path. Each shard is copied
    /// under its own lock, so the result is not one atomic snapshot.
    pub(super) fn latest_states(&self) -> Vec<(ObjectKey, LatestState)> {
        self.0
            .entries()
            .into_iter()
            .filter_map(|(_, path, entry)| entry.into_latest().map(|state| (path, state)))
            .collect()
    }
}

/// Downcasts a type-erased cached value to the codec's decoded type. A mismatch
/// means a path was used through the wrong typed store, which is an internal
/// error.
pub(super) fn downcast<C: Codec>(
    path: &ObjectPath,
    value: ErasedValue,
) -> Result<Arc<C::Value>, StorageError> {
    value.downcast::<C::Value>().map_err(|_| {
        StorageError::other(format!(
            "cached object at {path} has a different decoded type than {}",
            C::name()
        ))
    })
}
