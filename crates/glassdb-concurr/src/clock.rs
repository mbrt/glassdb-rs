//! A wall-clock abstraction that can be anchored to tokio's (mockable) time.
//!
//! The transaction monitor compares persisted timestamps (unix millis stored in
//! object tags) against "now" to decide whether a pending transaction has
//! expired. In production this is just `SystemTime::now`. In tests we want this
//! to advance together with `tokio::time::pause`/`advance`, so the [`Clock`]
//! can instead anchor a base wall-clock time to a base tokio `Instant` and
//! derive "now" from the (mocked) elapsed tokio time.

use std::time::SystemTime;

/// A source of wall-clock time.
#[derive(Clone)]
pub struct Clock(Kind);

#[derive(Clone)]
enum Kind {
    Real,
    /// Anchored to tokio's clock: `now = base_sys + (tokio::now - base_instant)`.
    Anchored {
        base_sys: SystemTime,
        base_instant: tokio::time::Instant,
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
        Clock(Kind::Anchored {
            base_sys: SystemTime::now(),
            base_instant: tokio::time::Instant::now(),
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
