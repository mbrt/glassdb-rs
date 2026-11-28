//! The latest known state of each path, and its type-safe cache transitions.

use std::sync::Arc;

use crate::error::StorageError;
use crate::timeline::{SequencePoint, Timeline};

use super::earlier::EarlierStates;
use super::entry::{DecodedCache, ErasedValue, LatestState, downcast};
use super::{Codec, Evidence, ObjectKey, Observation, Requirement, Revision};

/// The type-erased result shared by compatible readers of one flight.
#[derive(Clone)]
pub(super) struct FetchResult {
    value: Option<ErasedValue>,
    revision: Option<Revision>,
    evidence: Evidence,
}

impl FetchResult {
    /// The start of the installed state's currentness interval, which the
    /// persistent cache stores next to the watermark.
    pub(super) fn start(&self) -> Option<SequencePoint> {
        self.evidence.start()
    }

    /// Records that the persistent cache receives this state with watermark `t`.
    pub(super) fn persisted_at(&self, t: SequencePoint) {
        self.evidence.persisted_at(t);
    }
}

/// Present knowledge retained for a revision-conditional backend read.
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
            .is_some_and(|observation| observation.is_shared_with(&cached))
            || self
                .cached
                .as_ref()
                .is_some_and(|current| current.is_shared_with(&cached));
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

    fn capture_cached(&mut self, state: Option<LatestState>) {
        let cached = match (&self.predicate, state) {
            (ExpectedPredicate::Absent, Some(LatestState::Absent { evidence })) => Some(evidence),
            (
                ExpectedPredicate::Present(revision),
                Some(LatestState::Present {
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

    fn matches(&self, state: &LatestState) -> bool {
        match (&self.predicate, state) {
            (ExpectedPredicate::Absent, LatestState::Absent { .. }) => true,
            (
                ExpectedPredicate::Present(revision),
                LatestState::Present {
                    revision: current, ..
                },
            ) => revision == current,
            _ => false,
        }
    }
}

/// Owns the latest known state of each physical object path. A replaced leaf
/// state goes to the earlier states of its path in the same cache update.
#[derive(Clone)]
pub(super) struct LatestStates {
    cache: DecodedCache,
    earlier: EarlierStates,
    /// Marks the completion of the operations that observe a state, for the
    /// start of its currentness interval (ADR-097).
    timeline: Timeline,
}

impl LatestStates {
    pub(super) fn new(cache: DecodedCache, earlier: EarlierStates, timeline: Timeline) -> Self {
        Self {
            cache,
            earlier,
            timeline,
        }
    }

    /// Lists the cached present states with their paths, revisions, and
    /// evidence.
    pub(super) fn present_states(&self) -> Vec<(ObjectKey, Revision, Evidence)> {
        self.cache
            .latest_states()
            .into_iter()
            .filter_map(|(path, state)| match state {
                LatestState::Present {
                    revision, evidence, ..
                } => Some((path, revision, evidence)),
                LatestState::Absent { .. } => None,
            })
            .collect()
    }

    /// Returns a cached observation that satisfies `req` without backend I/O.
    pub(super) fn peek<C: Codec>(
        &self,
        key: &ObjectKey,
        req: Requirement,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let Some(state) = self.cache.latest(key.object_path()) else {
            return Ok(None);
        };
        match state {
            LatestState::Present {
                value,
                revision,
                evidence,
                ..
            } => {
                if !req.is_satisfied_by(evidence.get()) {
                    return Ok(None);
                }
                let value = downcast::<C>(key.object_path(), value)?;
                Ok(Some(Observation {
                    key: key.clone(),
                    value: Some(value),
                    revision: Some(revision),
                    evidence,
                }))
            }
            LatestState::Absent { evidence } => {
                if !req.is_satisfied_by(evidence.get()) {
                    return Ok(None);
                }
                Ok(Some(Observation {
                    key: key.clone(),
                    value: None,
                    revision: None,
                    evidence,
                }))
            }
        }
    }

    pub(super) fn present_seed<C: Codec>(
        &self,
        key: &ObjectKey,
        fallback: Option<&Observation<C::Value>>,
    ) -> Result<Option<PresentSeed>, StorageError> {
        if let Some(LatestState::Present {
            value,
            size,
            revision,
            evidence,
        }) = self.cache.latest(key.object_path())
        {
            downcast::<C>(key.object_path(), value.clone())?;
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
    ) -> FetchResult {
        let value = observed
            .value
            .as_ref()
            .map(|value| value.clone() as ErasedValue);
        FetchResult {
            value,
            revision: observed.revision.clone(),
            evidence: observed.evidence.clone(),
        }
    }

    pub(super) fn result_from_seed(&self, seed: PresentSeed) -> FetchResult {
        FetchResult {
            value: Some(seed.value),
            revision: Some(seed.revision),
            evidence: seed.evidence,
        }
    }

    pub(super) fn install_persistent<C: Codec>(
        &self,
        path: &ObjectKey,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
        start: Option<SequencePoint>,
    ) -> PresentSeed {
        let erased: ErasedValue = value;
        // The load is not an observation of this open, so it does not mark a
        // start. The state keeps the start that an earlier observation gave it.
        let incoming = Evidence::new(current_after);
        if let Some(start) = start {
            incoming.observed_by_completion(start);
        }
        incoming.persisted_at(current_after);
        let evidence = self.install_present(path, erased.clone(), size, revision.clone(), incoming);
        evidence.persisted_at(current_after);
        PresentSeed {
            value: erased,
            size,
            revision,
            evidence,
        }
    }

    pub(super) fn install_fetched<C: Codec>(
        &self,
        path: &ObjectKey,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
    ) -> FetchResult {
        let erased: ErasedValue = value;
        let evidence = self.install_observed::<C>(
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
        }
    }

    pub(super) fn install_mutation<C: Codec>(
        &self,
        key: ObjectKey,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        current_after: SequencePoint,
    ) -> Observation<C::Value> {
        let erased: ErasedValue = value.clone();
        let evidence = self.install_observed::<C>(
            &key,
            erased,
            size,
            revision.clone(),
            Evidence::new(current_after),
        );
        Observation {
            key,
            value: Some(value),
            revision: Some(revision),
            evidence,
        }
    }

    pub(super) fn install_absent_result(
        &self,
        path: &ObjectKey,
        current_after: SequencePoint,
    ) -> FetchResult {
        FetchResult {
            value: None,
            revision: None,
            evidence: self.install_absent(path, current_after),
        }
    }

    pub(super) fn install_absent_observation<V>(
        &self,
        key: ObjectKey,
        current_after: SequencePoint,
    ) -> Observation<V> {
        let evidence = self.install_absent(&key, current_after);
        Observation {
            key,
            value: None,
            revision: None,
            evidence,
        }
    }

    pub(super) fn confirm_unchanged<C: Codec>(
        &self,
        path: &ObjectKey,
        seed: PresentSeed,
        current_after: SequencePoint,
    ) -> FetchResult {
        seed.evidence.advance(current_after);
        let evidence = self.install_observed::<C>(
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
        }
    }

    pub(super) fn to_observation<C: Codec>(
        &self,
        key: ObjectKey,
        fetched: FetchResult,
    ) -> Result<Observation<C::Value>, StorageError> {
        let value = match fetched.value {
            Some(any) => Some(downcast::<C>(key.object_path(), any)?),
            None => None,
        };
        Ok(Observation {
            key,
            value,
            revision: fetched.revision,
            evidence: fetched.evidence,
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

    pub(super) fn capture_expected(&self, path: &ObjectKey, expected: &mut Expected) {
        expected.capture_cached(self.cache.latest(path.object_path()));
    }

    pub(super) fn advance_expected(&self, expected: &Expected, current_after: SequencePoint) {
        expected.advance(current_after);
    }

    pub(super) fn invalidate_expected(&self, path: &ObjectKey, expected: &Expected) {
        self.cache.update_latest(path, |old| match old {
            Some(state) if expected.matches(&state) => (None, ()),
            other => (other, ()),
        });
    }

    pub(super) fn invalidate(&self, path: &ObjectKey) {
        self.cache.delete_latest(path.object_path());
    }

    /// Installs a present state that a backend operation of this open
    /// observed. The completion of that operation starts its currentness
    /// interval.
    fn install_observed<C: Codec>(
        &self,
        path: &ObjectKey,
        value: ErasedValue,
        size: usize,
        revision: Revision,
        incoming: Evidence,
    ) -> Evidence {
        let installed = self.install_present(path, value, size, revision, incoming);
        installed.observed_by_completion(self.timeline.now());
        installed
    }

    /// Installs a present state, merging evidence when the latest state has
    /// the same revision. A different revision replaces the latest state. When
    /// a stale view read the path, the replaced state goes to the earlier
    /// states of the path in the same cache update (ADR-103).
    fn install_present(
        &self,
        path: &ObjectKey,
        value: ErasedValue,
        size: usize,
        revision: Revision,
        incoming: Evidence,
    ) -> Evidence {
        self.cache.update_both(path, |old, kept| match old {
            Some(LatestState::Present {
                value: old_value,
                size: old_size,
                revision: old_revision,
                evidence,
            }) if old_revision == revision => {
                evidence.advance(incoming.get());
                let installed = evidence.clone();
                let state = LatestState::Present {
                    value: old_value,
                    size: old_size,
                    revision: old_revision,
                    evidence,
                };
                (state, None, installed)
            }
            old => {
                let earlier = old
                    .zip(kept)
                    .map(|(replaced, kept)| self.earlier.after_replace(replaced, &kept));
                let installed = incoming.clone();
                let state = LatestState::Present {
                    value,
                    size,
                    revision,
                    evidence: incoming,
                };
                (state, earlier, installed)
            }
        })
    }

    /// Installs confirmed absence, merging evidence with an existing absence.
    fn install_absent(&self, path: &ObjectKey, current_after: SequencePoint) -> Evidence {
        let incoming = Evidence::new(current_after);
        let installed = self.cache.update_latest(path, |old| match old {
            Some(LatestState::Absent { evidence }) => {
                evidence.advance(current_after);
                let installed = evidence.clone();
                (Some(LatestState::Absent { evidence }), installed)
            }
            _ => {
                let installed = incoming.clone();
                (Some(LatestState::Absent { evidence: incoming }), installed)
            }
        });
        installed.observed_by_completion(self.timeline.now());
        installed
    }
}
