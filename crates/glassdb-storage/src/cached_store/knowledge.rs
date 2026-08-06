//! Decoded present/absent knowledge and type-safe cache transitions.

use std::any::Any;
use std::sync::Arc;

use crate::cache::{Cache, Weighable};
use crate::error::StorageError;
use crate::timeline::SequencePoint;

use super::{Codec, Evidence, Observation, Requirement, Revision, satisfies};

type ErasedValue = Arc<dyn Any + Send + Sync>;

/// One entry in the shared decoded LRU: either a present decoded value or a
/// confirmed absence. A missing object has no entry at all.
#[derive(Clone)]
enum EntryState {
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

#[derive(Clone)]
struct CacheEntry {
    state: EntryState,
}

impl Weighable for CacheEntry {
    fn size(&self) -> usize {
        // A present entry weighs its decoded size plus the revision token; an
        // absent entry costs a small fixed bookkeeping amount.
        const OVERHEAD: usize = std::mem::size_of::<CacheEntry>();
        match &self.state {
            EntryState::Present { size, revision, .. } => size + revision.0.token.len() + OVERHEAD,
            EntryState::Absent { .. } => OVERHEAD,
        }
    }
}

/// The type-erased result shared by compatible readers of one flight.
#[derive(Clone)]
pub(super) struct FetchResult {
    value: Option<ErasedValue>,
    revision: Option<Revision>,
    evidence: Evidence,
    cache_hit: bool,
}

/// Present knowledge retained for a version-conditional backend read.
#[derive(Clone)]
pub(super) struct PresentSeed {
    value: ErasedValue,
    size: usize,
    revision: Revision,
    evidence: Evidence,
}

impl PresentSeed {
    pub(super) fn revision(&self) -> &Revision {
        &self.revision
    }
}

enum ExpectedPredicate {
    Absent,
    Present(Revision),
}

/// Evidence cells proven current when a mutation predicate succeeds.
///
/// A retained observation and a matching cache entry can have distinct cells
/// after eviction and reload, so both must be preserved until reconciliation.
struct ExpectedEvidence {
    observation: Option<Evidence>,
    cached: Option<Evidence>,
}

impl ExpectedEvidence {
    fn new(observation: Option<Evidence>) -> Self {
        Self {
            observation,
            cached: None,
        }
    }

    fn capture_cached(&mut self, cached: Evidence) {
        let already_captured = self
            .observation
            .as_ref()
            .is_some_and(|observation| Arc::ptr_eq(&observation.0, &cached.0))
            || self
                .cached
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.0, &cached.0));
        if already_captured {
            return;
        }
        debug_assert!(
            self.cached.is_none(),
            "a mutation captures at most one matching cache entry"
        );
        self.cached = Some(cached);
    }

    fn advance(&self, invoked: SequencePoint) {
        for evidence in self.observation.iter().chain(self.cached.iter()) {
            evidence.advance(invoked);
        }
    }
}

/// The exact knowledge predicate and evidence retained by one mutation round.
pub(super) struct Expected {
    predicate: ExpectedPredicate,
    evidence: ExpectedEvidence,
}

impl Expected {
    fn absent(evidence: Option<Evidence>) -> Self {
        Self {
            predicate: ExpectedPredicate::Absent,
            evidence: ExpectedEvidence::new(evidence),
        }
    }

    fn present(revision: Revision, evidence: Evidence) -> Self {
        Self {
            predicate: ExpectedPredicate::Present(revision),
            evidence: ExpectedEvidence::new(Some(evidence)),
        }
    }

    fn capture_cached(&mut self, entry: Option<CacheEntry>) {
        let cached = match (&self.predicate, entry.map(|entry| entry.state)) {
            (ExpectedPredicate::Absent, Some(EntryState::Absent { evidence })) => Some(evidence),
            (
                ExpectedPredicate::Present(revision),
                Some(EntryState::Present {
                    revision: cached_revision,
                    evidence,
                    ..
                }),
            ) if *revision == cached_revision => Some(evidence),
            _ => None,
        };
        if let Some(cached) = cached {
            self.evidence.capture_cached(cached);
        }
    }

    fn advance(&self, invoked: SequencePoint) {
        self.evidence.advance(invoked);
    }

    fn matches(&self, entry: &CacheEntry) -> bool {
        match (&self.predicate, &entry.state) {
            (ExpectedPredicate::Absent, EntryState::Absent { .. }) => true,
            (
                ExpectedPredicate::Present(revision),
                EntryState::Present {
                    revision: current, ..
                },
            ) => revision == current,
            _ => false,
        }
    }
}

/// Owns all discoverable decoded knowledge for physical object paths.
#[derive(Clone)]
pub(super) struct Knowledge {
    cache: Arc<Cache<CacheEntry>>,
}

impl Knowledge {
    pub(super) fn new(max_size: usize) -> Self {
        Self {
            cache: Arc::new(Cache::new(max_size)),
        }
    }

    /// Returns a cached observation that satisfies `req` without backend I/O.
    pub(super) fn peek<C: Codec>(
        &self,
        path: &Arc<str>,
        req: Requirement,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let Some(entry) = self.cache.get(path) else {
            return Ok(None);
        };
        match entry.state {
            EntryState::Present {
                value,
                revision,
                evidence,
                ..
            } => {
                if !satisfies(evidence.get(), req) {
                    return Ok(None);
                }
                let value = downcast::<C>(path, value)?;
                Ok(Some(Observation {
                    path: path.clone(),
                    value: Some(value),
                    revision: Some(revision),
                    evidence,
                    cache_hit: true,
                }))
            }
            EntryState::Absent { evidence } => {
                if !satisfies(evidence.get(), req) {
                    return Ok(None);
                }
                Ok(Some(Observation {
                    path: path.clone(),
                    value: None,
                    revision: None,
                    evidence,
                    cache_hit: true,
                }))
            }
        }
    }

    pub(super) fn present_seed<C: Codec>(
        &self,
        path: &Arc<str>,
        fallback: Option<&Observation<C::Value>>,
    ) -> Result<Option<PresentSeed>, StorageError> {
        if let Some(CacheEntry {
            state:
                EntryState::Present {
                    value,
                    size,
                    revision,
                    evidence,
                },
        }) = self.cache.get(path)
        {
            downcast::<C>(path, value.clone())?;
            return Ok(Some(PresentSeed {
                value,
                size,
                revision,
                evidence,
            }));
        }
        let Some(observed) = fallback else {
            return Ok(None);
        };
        let (Some(value), Some(revision)) = (&observed.value, &observed.revision) else {
            return Ok(None);
        };
        let erased: ErasedValue = value.clone();
        Ok(Some(PresentSeed {
            value: erased,
            size: C::size(value),
            revision: revision.clone(),
            evidence: observed.evidence.clone(),
        }))
    }

    pub(super) fn result_from_observation<V: Send + Sync + 'static>(
        &self,
        observed: &Observation<V>,
        cache_hit: bool,
    ) -> FetchResult {
        let value = observed
            .value
            .as_ref()
            .map(|value| value.clone() as ErasedValue);
        FetchResult {
            value,
            revision: observed.revision.clone(),
            evidence: observed.evidence.clone(),
            cache_hit,
        }
    }

    pub(super) fn result_from_seed(&self, seed: PresentSeed, cache_hit: bool) -> FetchResult {
        FetchResult {
            value: Some(seed.value),
            revision: Some(seed.revision),
            evidence: seed.evidence,
            cache_hit,
        }
    }

    pub(super) fn install_persistent<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
    ) -> PresentSeed {
        let erased: ErasedValue = value;
        let evidence = self.install_present(
            path,
            erased.clone(),
            size,
            revision.clone(),
            Evidence::new(current_after),
        );
        PresentSeed {
            value: erased,
            size,
            revision,
            evidence,
        }
    }

    pub(super) fn install_fetched<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
    ) -> FetchResult {
        let erased: ErasedValue = value;
        let evidence = self.install_present(
            path,
            erased.clone(),
            size,
            revision.clone(),
            Evidence::new(current_after),
        );
        FetchResult {
            value: Some(erased),
            revision: Some(revision),
            evidence,
            cache_hit: false,
        }
    }

    pub(super) fn install_mutation<C: Codec>(
        &self,
        path: Arc<str>,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
    ) -> Observation<C::Value> {
        let erased: ErasedValue = value.clone();
        let evidence = self.install_present(
            &path,
            erased,
            size,
            revision.clone(),
            Evidence::new(current_after),
        );
        Observation {
            path,
            value: Some(value),
            revision: Some(revision),
            evidence,
            cache_hit: false,
        }
    }

    pub(super) fn install_absent_result(
        &self,
        path: &str,
        current_after: SequencePoint,
    ) -> FetchResult {
        FetchResult {
            value: None,
            revision: None,
            evidence: self.install_absent(path, current_after),
            cache_hit: false,
        }
    }

    pub(super) fn install_absent_observation<V>(
        &self,
        path: Arc<str>,
        current_after: SequencePoint,
    ) -> Observation<V> {
        let evidence = self.install_absent(&path, current_after);
        Observation {
            path,
            value: None,
            revision: None,
            evidence,
            cache_hit: false,
        }
    }

    pub(super) fn confirm_unchanged(
        &self,
        path: &str,
        seed: PresentSeed,
        current_after: SequencePoint,
    ) -> FetchResult {
        seed.evidence.advance(current_after);
        let evidence = self.install_present(
            path,
            seed.value.clone(),
            seed.size,
            seed.revision.clone(),
            seed.evidence,
        );
        FetchResult {
            value: Some(seed.value),
            revision: Some(seed.revision),
            evidence,
            cache_hit: true,
        }
    }

    pub(super) fn to_observation<C: Codec>(
        &self,
        path: &Arc<str>,
        fetched: FetchResult,
    ) -> Result<Observation<C::Value>, StorageError> {
        let value = match fetched.value {
            Some(any) => Some(downcast::<C>(path, any)?),
            None => None,
        };
        Ok(Observation {
            path: path.clone(),
            value,
            revision: fetched.revision,
            evidence: fetched.evidence,
            cache_hit: fetched.cache_hit,
        })
    }

    pub(super) fn expected_absent<V>(&self, observed: Option<&Observation<V>>) -> Expected {
        Expected::absent(observed.map(|observed| observed.evidence.clone()))
    }

    pub(super) fn expected_present<V>(
        &self,
        revision: Revision,
        observed: &Observation<V>,
    ) -> Expected {
        Expected::present(revision, observed.evidence.clone())
    }

    pub(super) fn capture_expected(&self, path: &str, expected: &mut Expected) {
        expected.capture_cached(self.cache.get(path));
    }

    pub(super) fn advance_expected(&self, expected: &Expected, current_after: SequencePoint) {
        expected.advance(current_after);
    }

    pub(super) fn invalidate_expected(&self, path: &str, expected: &Expected) {
        self.cache.update(path, |old| match old {
            Some(entry) if expected.matches(&entry) => None,
            other => other,
        });
    }

    pub(super) fn invalidate(&self, path: &str) {
        self.cache.delete(path);
    }

    /// Installs a present entry, merging evidence when the current entry has the
    /// same revision.
    fn install_present(
        &self,
        path: &str,
        value: ErasedValue,
        size: usize,
        revision: Revision,
        incoming: Evidence,
    ) -> Evidence {
        self.cache.update_with_result(path, |old| match old {
            Some(CacheEntry {
                state:
                    EntryState::Present {
                        value: old_value,
                        size: old_size,
                        revision: old_revision,
                        evidence,
                    },
            }) if old_revision == revision => {
                evidence.advance(incoming.get());
                let installed = evidence.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value: old_value,
                            size: old_size,
                            revision: old_revision,
                            evidence,
                        },
                    }),
                    installed,
                )
            }
            _ => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value,
                            size,
                            revision,
                            evidence: incoming,
                        },
                    }),
                    installed,
                )
            }
        })
    }

    /// Installs confirmed absence, merging evidence with an existing absence.
    fn install_absent(&self, path: &str, current_after: SequencePoint) -> Evidence {
        let incoming = Evidence::new(current_after);
        self.cache.update_with_result(path, |old| match old {
            Some(CacheEntry {
                state: EntryState::Absent { evidence },
            }) => {
                evidence.advance(current_after);
                let installed = evidence.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Absent { evidence },
                    }),
                    installed,
                )
            }
            _ => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Absent { evidence: incoming },
                    }),
                    installed,
                )
            }
        })
    }
}

/// Downcasts a type-erased cached value to the codec's decoded type. A mismatch
/// means a path was used through the wrong typed store, which is an internal
/// error.
fn downcast<C: Codec>(path: &str, value: ErasedValue) -> Result<Arc<C::Value>, StorageError> {
    value.downcast::<C::Value>().map_err(|_| {
        StorageError::other(format!(
            "cached object at {path} has a different decoded type than {}",
            C::name()
        ))
    })
}
