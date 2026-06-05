//! A wall-clock abstraction that can be anchored to tokio's (mockable) time.
//!
//! The transaction monitor compares persisted timestamps (unix millis stored in
//! object tags) against "now" to decide whether a pending transaction has
//! expired. In production this is just `SystemTime::now`. In tests we want this
//! to advance together with `tokio::time::pause`/`advance`, so the [`Clock`]
//! can instead anchor a base wall-clock time to a base `rt::Instant` and
//! derive "now" from the (mocked) elapsed time. Under `--cfg sim` the same anchor
//! follows the deterministic executor's virtual clock instead.

use std::time::SystemTime;

use crate::rt::Instant;

/// A source of wall-clock time.
#[derive(Clone)]
pub struct Clock(Kind);

#[derive(Clone)]
enum Kind {
    Real,
    /// Anchored to the runtime clock: `now = base_sys + (rt::now - base_instant)`.
    Anchored {
        base_sys: SystemTime,
        base_instant: Instant,
    },
}

impl Clock {
    /// A clock backed by the real system wall-clock.
    pub fn real() -> Self {
        Clock(Kind::Real)
    }

    /// A clock anchored to tokio's current time. Its `now()` advances together
    /// with tokio's clock, so `tokio::time::pause`/`advance` control it. Must be
    /// created inside a tokio runtime.
    pub fn anchored() -> Self {
        Self::anchored_at(SystemTime::now())
    }

    /// Like [`Clock::anchored`] but with an explicit base wall-clock time
    /// instead of `SystemTime::now()`. Under the deterministic simulation
    /// executor (`--cfg sim`) both the base instant (virtual time zero) and the
    /// elapsed time are deterministic, so a fixed `base_sys` makes `now()` — and
    /// therefore the transaction-id timestamps derived from it — a pure function
    /// of the simulation seed. Must be created inside a runtime (tokio or the
    /// simulation executor).
    pub fn anchored_at(base_sys: SystemTime) -> Self {
        Clock(Kind::Anchored {
            base_sys,
            base_instant: Instant::now(),
        })
    }

    /// Returns the current wall-clock time according to this clock.
    pub fn now(&self) -> SystemTime {
        match &self.0 {
            Kind::Real => SystemTime::now(),
            Kind::Anchored {
                base_sys,
                base_instant,
            } => *base_sys + base_instant.elapsed(),
        }
    }
}

impl Default for Clock {
    fn default() -> Self {
        Clock::real()
    }
}
