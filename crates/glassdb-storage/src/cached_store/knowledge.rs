//! Decoded present/absent knowledge and type-safe cache transitions.

use std::any::Any;
use std::sync::Arc;

use crate::cache::{Cache, EvictionHook, Weighable};
use crate::error::StorageError;
use crate::timeline::{FloorPoint, SequencePoint, Timeline, ViewFloors};

use super::{Codec, Evidence, ObjectKey, Observation, Requirement, Revision};

type ErasedValue = Arc<dyn Any + Send + Sync>;

/// Receives each present state that the decoded cache evicts for capacity,
/// so that the persistent cache can keep its newest watermark (ADR-097).
pub(super) trait EvictionSink: Send + Sync {
    fn evicted(&self, path: &str, revision: &Revision, evidence: &Evidence);
}

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
    /// The earlier states of the path that a live stale view can still use,
    /// newest first (ADR-103).
    earlier: EarlierStates,
}

impl CacheEntry {
    fn new(state: EntryState) -> Self {
        Self {
            state,
            earlier: EarlierStates::default(),
        }
    }
}

impl Weighable for CacheEntry {
    fn size(&self) -> usize {
        // A present entry weighs its decoded size plus the revision token; an
        // absent entry costs a small fixed bookkeeping amount.
        const OVERHEAD: usize = std::mem::size_of::<CacheEntry>();
        let current = match &self.state {
            EntryState::Present { size, revision, .. } => {
                size + revision.serialize().len() + OVERHEAD
            }
            EntryState::Absent { .. } => OVERHEAD,
        };
        current + self.earlier.size()
    }

    fn sheddable(&self) -> usize {
        self.earlier.size()
    }

    fn shed(&mut self) {
        self.earlier = EarlierStates::default();
    }
}

/// A present state of a path that a newer state replaced, kept with its
/// evidence. The evidence keeps growing: a CAS of this instance advances the
/// watermark of the state that it replaces.
#[derive(Clone)]
struct EarlierState {
    value: ErasedValue,
    size: usize,
    revision: Revision,
    evidence: Evidence,
}

/// The earlier states of one path, newest first. A cache read clones the
/// entry, so the states are shared.
#[derive(Clone, Default)]
struct EarlierStates(Arc<[EarlierState]>);

impl EarlierStates {
    fn size(&self) -> usize {
        const OVERHEAD: usize = std::mem::size_of::<EarlierState>();
        self.0
            .iter()
            .map(|state| state.size + state.revision.serialize().len() + OVERHEAD)
            .sum()
    }
}

/// The interval over which one part of a state kept the value that it has in
/// an observation, across the known states of the path (ADR-103).
pub(super) struct PartInterval {
    pub(super) start: SequencePoint,
    pub(super) watermark: SequencePoint,
    /// Whether the cached state of the path already has another value of the
    /// part.
    pub(super) cached_changed: bool,
}

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
    /// The floors of the live stale views, which decide the earlier states to
    /// keep.
    views: ViewFloors,
    /// Marks the completion of the operations that observe a state, for the
    /// start of its currentness interval (ADR-097).
    timeline: Timeline,
}

impl Knowledge {
    /// Creates the decoded cache. When `sink` is set, the cache passes to it
    /// each present state that it evicts for capacity.
    pub(super) fn new(
        max_size: usize,
        timeline: Timeline,
        views: ViewFloors,
        sink: Option<Arc<dyn EvictionSink>>,
    ) -> Self {
        let cache = match sink {
            Some(sink) => {
                let hook: EvictionHook<CacheEntry> = Box::new(move |path, entry| {
                    if let EntryState::Present {
                        revision, evidence, ..
                    } = entry.state
                    {
                        sink.evicted(path, &revision, &evidence);
                    }
                });
                Cache::with_eviction(max_size, hook)
            }
            None => Cache::new(max_size),
        };
        Self {
            cache: Arc::new(cache),
            views,
            timeline,
        }
    }

    pub(super) fn view_floors(&self) -> ViewFloors {
        self.views.clone()
    }

    /// Lists the cached present states with their paths, revisions, and
    /// evidence.
    pub(super) fn present_states(&self) -> Vec<(String, Revision, Evidence)> {
        self.cache
            .entries()
            .into_iter()
            .filter_map(|(path, entry)| match entry.state {
                EntryState::Present {
                    revision, evidence, ..
                } => Some((path, revision, evidence)),
                EntryState::Absent { .. } => None,
            })
            .collect()
    }

    /// Returns a cached observation that satisfies `req` without backend I/O.
    pub(super) fn peek<C: Codec>(
        &self,
        key: &ObjectKey,
        req: Requirement,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let Some(entry) = self.cache.get(key.as_str()) else {
            return Ok(None);
        };
        match entry.state {
            EntryState::Present {
                value,
                revision,
                evidence,
                ..
            } => {
                if !req.is_satisfied_by(evidence.get()) {
                    return Ok(None);
                }
                let value = downcast::<C>(key.as_str(), value)?;
                Ok(Some(Observation {
                    key: key.clone(),
                    value: Some(value),
                    revision: Some(revision),
                    evidence,
                }))
            }
            EntryState::Absent { evidence } => {
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

    /// Returns the earlier state of `key` whose currentness interval covers
    /// `point` and that satisfies `req`. Returns `None` when the cached state
    /// started at or before `point`, or when no earlier state covers it: then
    /// the cached state serves the read.
    pub(super) fn peek_earlier<C: Codec>(
        &self,
        key: &ObjectKey,
        req: Requirement,
        point: FloorPoint,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let point = point.point();
        let Some(entry) = self.cache.get(key.as_str()) else {
            return Ok(None);
        };
        if let EntryState::Present { evidence, .. } = &entry.state
            && evidence.start().is_some_and(|start| start <= point)
        {
            return Ok(None);
        }
        let Some(state) = entry.earlier.0.iter().find(|state| {
            state.evidence.start().is_some_and(|start| start <= point)
                && state.evidence.get() >= point
                && req.is_satisfied_by(state.evidence.get())
        }) else {
            return Ok(None);
        };
        Ok(Some(Observation {
            key: key.clone(),
            value: Some(downcast::<C>(key.as_str(), state.value.clone())?),
            revision: Some(state.revision.clone()),
            evidence: state.evidence.clone(),
        }))
    }

    /// Returns the interval over which the part of the state that `part`
    /// selects kept the value that it has in `observed`, from `observed`, the
    /// cached state, and the earlier states of its path. `part` must select a
    /// value that never comes back after it changes, and `None` from it ends
    /// the interval. Returns `None` for an absence, for a state with no start,
    /// and when `part` selects nothing in `observed`.
    pub(super) fn part_interval<C: Codec, S: PartialEq>(
        &self,
        observed: &Observation<C::Value>,
        part: impl Fn(&C::Value) -> Option<S>,
    ) -> Result<Option<PartInterval>, StorageError> {
        let Some(value) = observed.value() else {
            return Ok(None);
        };
        let (Some(mine), Some(mut start)) = (part(value), observed.evidence.start()) else {
            return Ok(None);
        };
        let mut watermark = observed.evidence.get();
        let mut cached_changed = false;
        if let Some(entry) = self.cache.get(observed.key.as_str()) {
            let mut extend =
                |value: &ErasedValue, evidence: &Evidence| -> Result<bool, StorageError> {
                    let value = downcast::<C>(observed.key.as_str(), value.clone())?;
                    let same = part(&value).as_ref() == Some(&mine);
                    if same {
                        if let Some(earlier) = evidence.start() {
                            start = start.min(earlier);
                        }
                        watermark = watermark.max(evidence.get());
                    }
                    Ok(same)
                };
            match &entry.state {
                EntryState::Present {
                    value, evidence, ..
                } => cached_changed = !extend(value, evidence)?,
                EntryState::Absent { .. } => cached_changed = true,
            }
            for state in entry.earlier.0.iter() {
                extend(&state.value, &state.evidence)?;
            }
        }
        Ok(Some(PartInterval {
            start,
            watermark,
            cached_changed,
        }))
    }

    pub(super) fn present_seed<C: Codec>(
        &self,
        key: &ObjectKey,
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
            ..
        }) = self.cache.get(key.as_str())
        {
            downcast::<C>(key.as_str(), value.clone())?;
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
        path: &str,
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
        let evidence =
            self.install_present::<C>(path, erased.clone(), size, revision.clone(), incoming);
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
        path: &str,
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
            key.as_str(),
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
        path: &str,
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
        let evidence = self.install_absent(key.as_str(), current_after);
        Observation {
            key,
            value: None,
            revision: None,
            evidence,
        }
    }

    pub(super) fn confirm_unchanged<C: Codec>(
        &self,
        path: &str,
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
            Some(any) => Some(downcast::<C>(key.as_str(), any)?),
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

    /// Installs a present state that a backend operation of this open
    /// observed. The completion of that operation starts its currentness
    /// interval.
    fn install_observed<C: Codec>(
        &self,
        path: &str,
        value: ErasedValue,
        size: usize,
        revision: Revision,
        incoming: Evidence,
    ) -> Evidence {
        let installed = self.install_present::<C>(path, value, size, revision, incoming);
        installed.observed_by_completion(self.timeline.now());
        installed
    }

    /// Installs a present entry, merging evidence when the current entry has the
    /// same revision. A different revision replaces the cached state, which
    /// becomes an earlier state when a live stale view can use it.
    fn install_present<C: Codec>(
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
                earlier,
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
                        earlier,
                    }),
                    installed,
                )
            }
            old => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value,
                            size,
                            revision,
                            evidence: incoming,
                        },
                        earlier: self.earlier_after::<C>(old),
                    }),
                    installed,
                )
            }
        })
    }

    /// Returns the earlier states to keep after `replaced` leaves the cache
    /// entry of its path. For the lowest floor of the live views, a path keeps
    /// the newest state that started at or before it, and every later state.
    /// An older state cannot cover the floor of any live view.
    fn earlier_after<C: Codec>(&self, replaced: Option<CacheEntry>) -> EarlierStates {
        let (Some(replaced), Some(lowest)) = (replaced, self.views.lowest()) else {
            return EarlierStates::default();
        };
        let mut states = Vec::with_capacity(replaced.earlier.0.len() + 1);
        if let EntryState::Present {
            value,
            size,
            revision,
            evidence,
        } = replaced.state
            && value
                .downcast_ref::<C::Value>()
                .is_some_and(C::keeps_earlier_states)
        {
            states.push(EarlierState {
                value,
                size,
                revision,
                evidence,
            });
        }
        states.extend(replaced.earlier.0.iter().cloned());
        if let Some(last) = states
            .iter()
            .position(|state| state.evidence.start().is_some_and(|start| start <= lowest))
        {
            states.truncate(last + 1);
        }
        EarlierStates(states.into())
    }

    /// Installs confirmed absence, merging evidence with an existing absence.
    fn install_absent(&self, path: &str, current_after: SequencePoint) -> Evidence {
        let incoming = Evidence::new(current_after);
        let installed = self.cache.update_with_result(path, |old| match old {
            Some(CacheEntry {
                state: EntryState::Absent { evidence },
                ..
            }) => {
                evidence.advance(current_after);
                let installed = evidence.clone();
                (
                    Some(CacheEntry::new(EntryState::Absent { evidence })),
                    installed,
                )
            }
            _ => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry::new(EntryState::Absent { evidence: incoming })),
                    installed,
                )
            }
        });
        installed.observed_by_completion(self.timeline.now());
        installed
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
