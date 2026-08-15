//! Native runtime adapters.

use std::future::Future;
use std::ops::Add;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use super::TimedOut;

pub use tokio::task::JoinHandle;
pub use tokio::task::yield_now;

/// Error returned when process-wide model time cannot be configured.
#[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
pub enum ModelTimeError {
    /// The requested speedup cannot represent a coherent time conversion.
    #[error("model-time speedup must be finite, positive, and representable, got {0}")]
    InvalidSpeedup(f64),
    /// Model time was already configured or observed by this process.
    #[error("model time was already initialized")]
    AlreadyInitialized,
}

/// Installs an immutable process-wide model-time speedup.
///
/// A speedup of `N` makes one wall second advance model time by `N` seconds.
/// The setting must be installed before any model-time observation or wait.
/// Explicit configuration also anchors wall timestamps to model monotonic time;
/// an unconfigured process defaults to live real time at speed `1`.
pub fn set_model_time_speedup(speedup: f64) -> Result<(), ModelTimeError> {
    if !(speedup.is_finite()
        && speedup > 0.0
        && Duration::try_from_secs_f64(speedup).is_ok()
        && Duration::try_from_secs_f64(1.0 / speedup).is_ok())
    {
        return Err(ModelTimeError::InvalidSpeedup(speedup));
    }
    MODEL_TIME
        .set(ModelTime {
            speedup,
            anchored_wall: true,
        })
        .map_err(|_| ModelTimeError::AlreadyInitialized)
}

#[derive(Clone, Copy)]
struct ModelTime {
    speedup: f64,
    anchored_wall: bool,
}

static MODEL_TIME: OnceLock<ModelTime> = OnceLock::new();

fn model_time() -> &'static ModelTime {
    MODEL_TIME.get_or_init(|| ModelTime {
        speedup: 1.0,
        anchored_wall: false,
    })
}

fn scaled_duration(duration: Duration, factor: f64) -> Duration {
    if duration.is_zero() || factor == 1.0 {
        return duration;
    }
    Duration::try_from_secs_f64(duration.as_secs_f64() * factor).unwrap_or(Duration::MAX)
}

fn model_elapsed(duration: Duration) -> Duration {
    scaled_duration(duration, model_time().speedup)
}

fn model_wait(duration: Duration) -> Duration {
    if duration.is_zero() {
        return duration;
    }
    let wait = scaled_duration(duration, 1.0 / model_time().speedup);
    // Preserve an actual scheduling point even when acceleration rounds a
    // nominally nonzero duration below the runtime's nanosecond representation.
    wait.max(Duration::from_nanos(1))
}

/// A monotonic instant whose elapsed durations are expressed in model time.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Instant(tokio::time::Instant);

impl Instant {
    pub fn now() -> Self {
        // Observing an instant freezes the process-wide configuration even
        // though the raw instant itself needs no conversion yet.
        let _ = model_time();
        Instant(tokio::time::Instant::now())
    }

    pub fn elapsed(&self) -> Duration {
        model_elapsed(self.0.elapsed())
    }

    pub fn duration_since(&self, earlier: Instant) -> Duration {
        model_elapsed(self.0.duration_since(earlier.0))
    }

    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        model_elapsed(self.0.saturating_duration_since(earlier.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Instant {
        Instant(self.0 + model_wait(rhs))
    }
}

/// Reports whether the deterministic executor is active.
pub fn in_sim() -> bool {
    false
}

struct WallAnchor {
    system: SystemTime,
    instant: Instant,
}

/// The current wall-clock time in the process's model-time domain.
pub fn system_now() -> SystemTime {
    if !model_time().anchored_wall {
        return SystemTime::now();
    }
    static ANCHOR: OnceLock<WallAnchor> = OnceLock::new();
    let anchor = ANCHOR.get_or_init(|| WallAnchor {
        system: SystemTime::now(),
        instant: Instant::now(),
    });
    anchor
        .system
        .checked_add(anchor.instant.elapsed())
        .expect("model wall time exceeded SystemTime's range")
}

/// Sleeps for `duration` in model time.
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(model_wait(duration)).await;
}

/// Returns the host's available parallelism.
pub fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
}

/// Spawns a task on the ambient tokio runtime.
pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(f)
}

/// Runs `future` until it completes or `duration` elapses.
pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, TimedOut>
where
    F: Future,
{
    tokio::time::timeout(model_wait(duration), future)
        .await
        .map_err(|_| TimedOut)
}
