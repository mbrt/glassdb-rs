//! One change of one object by a single writer: load the object, decide,
//! write it with a CAS, and act on the result.
//!
//! A landed CAS proves the state that the change decided on, because a CAS
//! rejects a stale state. A change that writes nothing has no such check. So
//! the step accepts a result without a write only from a load that satisfies
//! the freshness requirement that the caller gives as proof (see the
//! capture-point rules in
//! `docs/guides/caching.md`).

use std::future::Future;
use std::sync::Arc;

use glassdb_concurr::{RetrySchedule, rt};

use super::{CasResult, Codec, ObjectKey, Observation, TypedCachedStore};
use crate::error::StorageError;
use crate::timeline::Requirement;

/// What a change decides on one loaded state of its object.
// One value exists for each load of a step, and only until the step acts on
// it, so the size of its largest variant costs nothing.
#[allow(clippy::large_enum_variant)]
pub enum Decision<V, T> {
    /// Write this changed state with a CAS. The step returns `T` when the CAS
    /// lands, and loads the object again when the CAS loses.
    Write(V, T),
    /// Write this changed state with a CAS, and decide again after it lands,
    /// on a load at [`Requirement::ANY`]. That load gives the state that the
    /// CAS installed, or a newer one, with no backend call. The step loads the
    /// object again when the CAS loses. Use it for a change that needs more
    /// than one write, each on the result of the one before.
    Advance(V),
    /// The change ends without a write, with a result that a newer state of
    /// the object can change, for example because the object is already as
    /// the change needs it. The step returns `T` only if the loaded state
    /// satisfies `proof`, the freshness requirement of the caller. Otherwise it
    /// loads the object again at that requirement and decides again.
    Done(T),
    /// The change ends without a write, with a result that the caller can
    /// accept also from an older state, for example when it gives up the
    /// change. The step returns `T` at once.
    Stop(T),
    /// Load the object again at this requirement and decide again, for
    /// example after a wait in the decision.
    Reload(Requirement),
    /// Wait for the next delay of the backoff of the step, then load the
    /// object again at this requirement and decide again. A step with
    /// [`CasRetries::Bounded`] has no backoff and loads at once.
    Wait(Requirement),
}

/// The result of a converge step.
// One value exists for each step, and only until its caller reads it.
#[allow(clippy::large_enum_variant)]
pub enum Converged<V, T> {
    /// The CAS of a [`Decision::Write`] landed. `state` is the written state
    /// and `observation` is the state that the CAS installed.
    Written {
        value: T,
        state: V,
        observation: Observation<V>,
    },
    /// A [`Decision::Done`] from a state that satisfies `proof`.
    Done(T),
    /// A [`Decision::Stop`].
    Stop(T),
    /// The object is missing, in a state that satisfies `proof`.
    Missing,
    /// The [`CasRetries::Bounded`] number of CASes lost.
    Exhausted,
}

/// How many lost CASes a step accepts, and how long it waits after a lost CAS
/// and for a [`Decision::Wait`].
#[derive(Clone, Copy)]
pub enum CasRetries {
    /// The step ends with [`Converged::Exhausted`] after this many lost CASes.
    /// It does not wait.
    Bounded(usize),
    /// The step accepts any number of lost CASes. It waits with delays from
    /// this schedule after each lost CAS and for each [`Decision::Wait`], from
    /// one backoff for the whole step.
    Unbounded(RetrySchedule),
}

impl<C: Codec> TypedCachedStore<C>
where
    C::Value: Clone,
{
    /// Changes the object at `key` until `decide` gives a result.
    ///
    /// The first load is at [`Requirement::ANY`], so a cached state costs no
    /// backend call, and so is each load after a CAS. `decide` gets a copy of
    /// the loaded state and its observation. A [`Decision::Done`] and a
    /// missing object need a state that satisfies `proof`, so the step loads
    /// again at `proof` when the loaded state does not satisfy it.
    /// [`Decision::Reload`] and [`Decision::Wait`] set the requirement of the
    /// next load.
    ///
    /// An in-doubt CAS is an error.
    pub(crate) async fn converge<T, E, F, Fut>(
        &self,
        key: impl Into<ObjectKey>,
        proof: Requirement,
        retries: CasRetries,
        mut decide: F,
    ) -> Result<Converged<C::Value, T>, E>
    where
        E: From<StorageError>,
        F: FnMut(C::Value, Observation<C::Value>) -> Fut,
        Fut: Future<Output = Result<Decision<C::Value, T>, E>>,
    {
        let key = key.into();
        let mut backoff = match retries {
            CasRetries::Bounded(_) => None,
            CasRetries::Unbounded(schedule) => Some(schedule.backoff()),
        };
        let mut lost = 0;
        let mut requirement = Requirement::ANY;
        loop {
            let observation = self.read(key.clone(), requirement).await?;
            let proven = observation.satisfies(proof);
            let Some(loaded) = observation.value() else {
                if proven {
                    return Ok(Converged::Missing);
                }
                requirement = proof;
                continue;
            };
            let state = loaded.as_ref().clone();
            // A decision can wait for other transactions, and its future can be
            // large. Inlined in the future of every caller, it overflows the
            // stack of a debug build (the `database_options` tests). One
            // allocation for each decision costs little next to the load.
            let (state, value) = match Box::pin(decide(state, observation.clone())).await? {
                Decision::Write(state, value) => (state, Some(value)),
                Decision::Advance(state) => (state, None),
                Decision::Done(value) if proven => return Ok(Converged::Done(value)),
                Decision::Done(_) => {
                    requirement = proof;
                    continue;
                }
                Decision::Stop(value) => return Ok(Converged::Stop(value)),
                Decision::Reload(next) => {
                    requirement = next;
                    continue;
                }
                Decision::Wait(next) => {
                    if let Some(backoff) = backoff.as_mut() {
                        rt::sleep(backoff.next_delay()).await;
                    }
                    requirement = next;
                    continue;
                }
            };
            match self.replace(&observation, Arc::new(state.clone())).await {
                Ok(CasResult::Applied(receipt)) => match value {
                    Some(value) => {
                        return Ok(Converged::Written {
                            value,
                            state,
                            observation: receipt.into_installed(),
                        });
                    }
                    None => {
                        requirement = Requirement::ANY;
                        continue;
                    }
                },
                Ok(CasResult::Rejected) | Err(StorageError::NotFound) => {}
                Err(error) => return Err(error.into()),
            }
            lost += 1;
            match (retries, backoff.as_mut()) {
                (CasRetries::Bounded(attempts), _) if lost >= attempts => {
                    return Ok(Converged::Exhausted);
                }
                (_, Some(backoff)) => rt::sleep(backoff.next_delay()).await,
                _ => {}
            }
            // The lost CAS removed the stale state from the cache.
            requirement = Requirement::ANY;
        }
    }
}
