//! The earlier states of the paths that the stale views can use (ADR-103).

use std::sync::Arc;

use crate::error::StorageError;
use crate::stale_view::{FloorPoint, ViewFloors};
use crate::timeline::SequencePoint;

use super::entry::{
    DecodedCache, EarlierState, ErasedValue, LatestState, PathEarlierStates, downcast,
};
use super::{Codec, Evidence, ObjectKey, Observation, Requirement};

/// The interval over which one part of a state kept the value that it has in
/// an observation, across the known states of the path (ADR-103).
pub(super) struct PartInterval {
    pub(super) start: SequencePoint,
    pub(super) watermark: SequencePoint,
    /// Whether the latest state of the path already has another value of the
    /// part.
    pub(super) cached_changed: bool,
}

/// Keeps, in the decoded cache, the replaced states that the floors of the
/// live stale views can use.
#[derive(Clone)]
pub(super) struct EarlierStates {
    cache: DecodedCache,
    /// The floors of the live stale views, which decide the earlier states to
    /// keep.
    views: ViewFloors,
}

impl EarlierStates {
    pub(super) fn new(cache: DecodedCache, views: ViewFloors) -> Self {
        Self { cache, views }
    }

    pub(super) fn view_floors(&self) -> ViewFloors {
        self.views.clone()
    }

    /// Makes the cache keep the replaced states of `key`, by giving the path
    /// an entry of earlier states, empty at first. The cache captures a
    /// replaced state only for a path with that entry, so only for an object
    /// that a stale view read.
    pub(super) fn keep(&self, key: &ObjectKey) {
        if self.earlier(key).is_some() {
            return;
        }
        self.cache.update_earlier(key.as_str(), |old| {
            old.or_else(|| Some(PathEarlierStates(Arc::from([]))))
        });
    }

    /// Removes the earlier states of `key` and its mark.
    pub(super) fn drop(&self, key: &ObjectKey) {
        self.cache.delete_earlier(key.as_str());
    }

    /// Returns the earlier states of a path after `replaced` stops being its
    /// latest state, from `kept`, the earlier states that the path had. For
    /// each floor of the live views, a path keeps the newest state that
    /// started at or before it, and the next newer state. The first covers the
    /// floor when the cache has a state that covers it, and gives the key
    /// intervals of the floor their earliest start. The second ends the
    /// interval of a key that the write after the floor did not change, also
    /// when the first has no confirmation after the floor. While a floor is
    /// raised, the path also keeps its newest earlier state: a call that read
    /// it can still raise its floor to a point that only that state covers.
    /// The result can be empty: the path then still keeps its next replaced
    /// state.
    pub(super) fn after_replace(
        &self,
        replaced: LatestState,
        kept: &PathEarlierStates,
    ) -> PathEarlierStates {
        let mut states = Vec::with_capacity(kept.0.len() + 1);
        if let LatestState::Present {
            value,
            size,
            revision,
            evidence,
        } = replaced
        {
            states.push(EarlierState {
                value,
                size,
                revision,
                evidence,
            });
        }
        states.extend(kept.0.iter().cloned());
        if states.is_empty() {
            return kept.clone();
        }
        let floors = self.views.raised();
        let mut keep = vec![false; states.len()];
        keep[0] = !floors.is_empty();
        for floor in floors {
            if let Some(newest) = states
                .iter()
                .position(|state| state.evidence.start().is_some_and(|start| start <= floor))
            {
                keep[newest] = true;
                // States are newest first, so the next newer state comes
                // before it.
                if let Some(next) = newest.checked_sub(1) {
                    keep[next] = true;
                }
            }
        }
        let mut keep = keep.into_iter();
        states.retain(|_| keep.next().unwrap_or(false));
        PathEarlierStates(states.into())
    }

    /// Returns the earlier state of `key` whose currentness interval covers
    /// `point` and that satisfies `req`. Returns `None` when the latest state
    /// started at or before `point`, or when no earlier state covers it: then
    /// the latest state serves the read.
    pub(super) fn state_at<C: Codec>(
        &self,
        key: &ObjectKey,
        req: Requirement,
        point: FloorPoint,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let point = point.point();
        if let Some(LatestState::Present { evidence, .. }) = self.latest(key)
            && evidence.start().is_some_and(|start| start <= point)
        {
            return Ok(None);
        }
        let Some(states) = self.earlier(key) else {
            return Ok(None);
        };
        let Some(state) = states.0.iter().find(|state| {
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
    /// latest state, and the earlier states of its path. The interval spans
    /// every known state with the same value of the part, also across states
    /// with another value or none, so `part` must select a value that never
    /// comes back after it changes. Returns `None` for an absence, for a state
    /// with no start, and when `part` selects nothing in `observed`.
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
        let mut extend = |value: &ErasedValue, evidence: &Evidence| -> Result<bool, StorageError> {
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
        let cached_changed = match self.latest(&observed.key) {
            Some(LatestState::Present {
                value, evidence, ..
            }) => !extend(&value, &evidence)?,
            Some(LatestState::Absent { .. }) => true,
            None => false,
        };
        if let Some(states) = self.earlier(&observed.key) {
            for state in states.0.iter() {
                extend(&state.value, &state.evidence)?;
            }
        }
        Ok(Some(PartInterval {
            start,
            watermark,
            cached_changed,
        }))
    }

    fn latest(&self, key: &ObjectKey) -> Option<LatestState> {
        self.cache.latest(key.as_str())
    }

    fn earlier(&self, key: &ObjectKey) -> Option<PathEarlierStates> {
        self.cache.earlier(key.as_str())
    }
}
