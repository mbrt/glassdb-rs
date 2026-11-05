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

/// The model-time speedup of a paused clock. Tokio rounds each timer up to a
/// whole millisecond of its clock, so a paused clock that runs 1000 times as
/// fast as model time makes model waits exact to a microsecond.
const PAUSED_SPEEDUP: f64 = 0.001;

/// Installs process-wide model time for a runtime whose clock is paused, so
/// that model time advances only when the runtime is idle.
///
/// Model time then follows the paused clock, with a finer resolution than the
/// timers of that clock. The paused clock still is not wall time, so
/// `tokio::time` waits and instants do not show model time; use the waits and
/// instants of this module. The setting must be installed before any
/// model-time observation or wait.
pub fn set_paused_model_time() -> Result<(), ModelTimeError> {
    set_model_time_speedup(PAUSED_SPEEDUP)
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

/// The resolution of Tokio timers. On a live clock, the driver parks for a
/// timeout of whole milliseconds, rounded up, so a timer of `k` whole
/// milliseconds wakes after about `k + 1` ms (measured on Linux with
/// Tokio 1.x).
const TIMER_TICK: Duration = Duration::from_millis(1);

/// Returns the request that makes a live Tokio timer wake as soon as possible
/// after `wall`, but not before it: the wait rounded up to whole ticks, less
/// one tick. The wait then ends about 70 µs after a whole tick, so it is late
/// by less than one tick, instead of by up to two.
fn live_timer_request(wall: Duration) -> Duration {
    let tick = TIMER_TICK.as_nanos();
    let ticks = wall.as_nanos().div_ceil(tick);
    let request = ticks.saturating_sub(1) * tick;
    // A zero request can fire before the driver parks. One microsecond makes
    // the driver park for its shortest timeout, one tick.
    u64::try_from(request)
        .map(Duration::from_nanos)
        // A request is shorter than the wait, so the wait bounds it.
        .unwrap_or(wall)
        .max(Duration::from_micros(1))
}

/// Returns the two timers of a wait for `wall`, a duration of the runtime
/// clock: an early timer from [`live_timer_request`], then the deadline.
///
/// The early timer removes most of the lag of a live clock. It can wake
/// before the deadline: a paused clock wakes each timer on its tick, and a
/// busy runtime turns its driver between parks. The deadline timer then
/// waits for the rest, so a wait is never shorter than `wall`.
///
/// Returns `None` when one Tokio timer is enough: virtual time runs its clock
/// 1000 times as fast as model time, so its lag is already too small to
/// matter, and Tokio clamps a deadline that it cannot represent.
fn live_timers(wall: Duration) -> Option<(tokio::time::Instant, tokio::time::Instant)> {
    if model_time().speedup == PAUSED_SPEEDUP {
        return None;
    }
    let start = tokio::time::Instant::now();
    let deadline = start.checked_add(wall)?;
    let early = (start + live_timer_request(wall)).min(deadline);
    Some((early, deadline))
}

/// Sleeps for `duration` in model time.
pub async fn sleep(duration: Duration) {
    let wall = model_wait(duration);
    let Some((early, deadline)) = live_timers(wall) else {
        return tokio::time::sleep(wall).await;
    };
    tokio::time::sleep_until(early).await;
    if tokio::time::Instant::now() < deadline {
        tokio::time::sleep_until(deadline).await;
    }
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
    let wall = model_wait(duration);
    let Some((early, deadline)) = live_timers(wall) else {
        return tokio::time::timeout(wall, future)
            .await
            .map_err(|_| TimedOut);
    };
    let mut future = std::pin::pin!(future);
    if let Ok(output) = tokio::time::timeout_at(early, &mut future).await {
        return Ok(output);
    }
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| TimedOut)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::live_timer_request;

    #[test]
    fn live_timer_request_is_one_tick_less_than_the_rounded_wait() {
        let us = Duration::from_micros;
        let ms = Duration::from_millis;
        // A wait of one tick or less needs the shortest park.
        assert_eq!(live_timer_request(Duration::from_nanos(1)), us(1));
        assert_eq!(live_timer_request(ms(1)), us(1));
        // A whole number of ticks parks one tick more than the request.
        assert_eq!(live_timer_request(ms(20)), ms(19));
        // A fractional wait ends at the end of its last tick.
        assert_eq!(live_timer_request(us(1001)), ms(1));
        assert_eq!(live_timer_request(us(4400)), ms(4));
        // The wait bounds a request that nanoseconds cannot hold.
        assert_eq!(live_timer_request(Duration::MAX), Duration::MAX);
        let long = Duration::from_secs(u64::MAX / 1_000_000_000 + 1);
        assert_eq!(live_timer_request(long), long);
    }
}
