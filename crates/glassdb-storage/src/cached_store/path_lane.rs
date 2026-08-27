//! Same-path admission and compatible read-flight coordination.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use glassdb_concurr::shard::Sharded;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use super::Requirement;
use super::knowledge::FetchResult;
use super::persistent_bridge::PersistentPath;
use crate::error::StorageError;
use crate::timeline::SequencePoint;

#[derive(Clone)]
pub(super) enum FlightOutcome {
    Success(FetchResult),
    Error(StorageError),
    Cancelled,
}

struct InFlight {
    invoked: SequencePoint,
    outcome: Mutex<Option<FlightOutcome>>,
    notify: Notify,
}

impl InFlight {
    async fn wait(&self) -> FlightOutcome {
        loop {
            let notified = self.notify.notified();
            if let Some(outcome) = self.outcome.lock().unwrap().clone() {
                return outcome;
            }
            notified.await;
        }
    }

    fn finish(&self, outcome: FlightOutcome) {
        let mut slot = self.outcome.lock().unwrap();
        if slot.is_none() {
            *slot = Some(outcome);
            self.notify.notify_waiters();
        }
    }
}

// Coordination has a different lifetime from cached knowledge, so it uses the
// same sharding policy as the cache without sharing its storage or locks.
type PathMapShard = Mutex<HashMap<Arc<str>, Weak<PathState>>>;
type PathMap = Sharded<PathMapShard>;

/// Database-local admission for actual backend calls on physical paths.
#[derive(Clone)]
pub(super) struct PathCoordinator {
    paths: Arc<PathMap>,
}

impl PathCoordinator {
    pub(super) fn new() -> Self {
        Self {
            paths: Arc::new(Sharded::new(|_| Mutex::new(HashMap::new()))),
        }
    }

    pub(super) fn state(&self, path: &Arc<str>) -> Arc<PathState> {
        let mut paths = self.paths.for_key(path.as_bytes()).lock().unwrap();
        if let Some(state) = paths.get(path.as_ref()).and_then(Weak::upgrade) {
            return state;
        }
        let state = Arc::new(PathState {
            path: path.clone(),
            coordinator: Arc::downgrade(&self.paths),
            gate: Arc::new(Semaphore::new(1)),
            flight: Mutex::new(None),
            persistent: PersistentPath::default(),
        });
        paths.insert(path.clone(), Arc::downgrade(&state));
        state
    }

    pub(super) async fn acquire(&self, path: &Arc<str>) -> PathPermit {
        let state = self.state(path);
        let permit = state
            .gate
            .clone()
            .acquire_owned()
            .await
            .expect("path semaphores are never closed");
        PathPermit {
            state,
            permit: Some(permit),
        }
    }

    pub(super) async fn admit_read(
        &self,
        path: &Arc<str>,
        requirement: Requirement,
    ) -> ReadAdmission {
        let state = self.state(path);
        let flight = state.flight.lock().unwrap().clone();
        if let Some(flight) = flight.filter(|flight| requirement.is_satisfied_by(flight.invoked)) {
            return ReadAdmission::Join(ReadWaiter { flight });
        }
        let permit = state
            .gate
            .clone()
            .acquire_owned()
            .await
            .expect("path semaphores are never closed");
        ReadAdmission::Lead(PathPermit {
            state,
            permit: Some(permit),
        })
    }

    #[cfg(test)]
    fn tracked_path_count(&self) -> usize {
        let mut count = 0;
        self.paths
            .each(|paths| count += paths.lock().unwrap().len());
        count
    }
}

pub(super) struct PathState {
    path: Arc<str>,
    coordinator: Weak<PathMap>,
    gate: Arc<Semaphore>,
    flight: Mutex<Option<Arc<InFlight>>>,
    persistent: PersistentPath,
}

impl PathState {
    pub(super) fn persistent(&self) -> &PersistentPath {
        &self.persistent
    }
}

impl Drop for PathState {
    fn drop(&mut self) {
        let Some(paths) = self.coordinator.upgrade() else {
            return;
        };
        let mut paths = paths.for_key(self.path.as_bytes()).lock().unwrap();
        if paths
            .get(self.path.as_ref())
            .is_some_and(|state| state.upgrade().is_none())
        {
            paths.remove(self.path.as_ref());
        }
    }
}

pub(super) struct PathPermit {
    state: Arc<PathState>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PathPermit {
    pub(super) fn state(&self) -> &Arc<PathState> {
        &self.state
    }

    pub(super) fn lead_read(self, invoked: SequencePoint) -> FlightLeader {
        let flight = Arc::new(InFlight {
            invoked,
            outcome: Mutex::new(None),
            notify: Notify::new(),
        });
        let previous = self.state.flight.lock().unwrap().replace(flight.clone());
        assert!(
            previous.is_none(),
            "path permit had an existing read flight"
        );
        FlightLeader {
            permit: Some(self),
            flight,
            armed: true,
        }
    }
}

impl Drop for PathPermit {
    fn drop(&mut self) {
        self.permit.take();
    }
}

pub(super) struct ReadWaiter {
    flight: Arc<InFlight>,
}

impl ReadWaiter {
    pub(super) async fn wait(&self) -> FlightOutcome {
        self.flight.wait().await
    }
}

pub(super) enum ReadAdmission {
    Join(ReadWaiter),
    Lead(PathPermit),
}

pub(super) struct FlightLeader {
    permit: Option<PathPermit>,
    flight: Arc<InFlight>,
    armed: bool,
}

impl FlightLeader {
    pub(super) fn complete(mut self, outcome: FlightOutcome) {
        self.flight.finish(outcome);
        self.remove();
        self.armed = false;
        self.permit.take();
    }

    fn remove(&self) {
        let Some(permit) = &self.permit else {
            return;
        };
        let mut flight = permit.state.flight.lock().unwrap();
        if flight
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &self.flight))
        {
            flight.take();
        }
    }
}

impl Drop for FlightLeader {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.flight.finish(FlightOutcome::Cancelled);
        self.remove();
        self.permit.take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn released_path_states_are_removed_from_the_weak_registry() {
        let coordinator = PathCoordinator::new();

        for index in 0..1_000 {
            let path: Arc<str> = Arc::from(format!("path/{index}"));
            let permit = coordinator.acquire(&path).await;
            let same_state = coordinator.state(&path);

            assert!(Arc::ptr_eq(permit.state(), &same_state));
            assert_eq!(coordinator.tracked_path_count(), 1);

            drop(same_state);
            drop(permit);
            assert_eq!(coordinator.tracked_path_count(), 0);
        }
    }
}
