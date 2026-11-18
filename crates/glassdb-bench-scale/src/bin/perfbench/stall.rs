//! A watchdog for a cell that stops model time in virtual time.
//!
//! In virtual time, model time advances only when no task can run. A task
//! that runs in a loop at one model instant, for example a livelock with no
//! backend operation, stops model time, and the measurement never ends. The
//! watchdog runs on its own thread, so that it sees such a loop: it reads the
//! model clock of the runtime once a second of wall time. When model time
//! does not advance for [`STALL_LIMIT`] while a cell runs, it reports the cell
//! and ends the process. The thread of the runtime cannot stop the loop, so
//! the process cannot return.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use glassdb_concurr::rt;
use tokio::runtime::Handle;

/// The wall time without an advance of model time that fails a cell. The
/// longest that a cell of the experiments stops model time is about 1.8 s, in
/// a release build: the CPU work of many workers at one model instant. The
/// limit leaves room for slower builds and machines.
const STALL_LIMIT: Duration = Duration::from_secs(60);

/// The wall time between two reads of the model clock.
const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The name of the cell that runs, with its shapes, while the cells of a
/// chain run. The last cell stays named while its instances close. Outside a
/// chain, the runtime can be idle, and then model time does not advance, so
/// the watchdog checks only while a chain runs.
static CELL: Mutex<Option<String>> = Mutex::new(None);

/// Records that the cell `name` runs from now on, until the next cell or the
/// end of the chain.
pub(crate) fn enter_cell(name: String) {
    *lock() = Some(name);
}

/// Returns a guard that records the end of the chain when it is dropped, also
/// when the chain fails.
pub(crate) fn watch_chain() -> ChainGuard {
    ChainGuard(())
}

/// The cells of a chain run while it is alive.
pub(crate) struct ChainGuard(());

impl Drop for ChainGuard {
    fn drop(&mut self) {
        *lock() = None;
    }
}

/// Starts the watchdog for the model clock of `runtime`. When a cell stops
/// model time, it calls `report` with the error, and then ends the process
/// with a failed exit status.
pub(crate) fn watch(runtime: Handle, report: impl Fn(&str) + Send + 'static) {
    std::thread::spawn(move || {
        // The model clock of the runtime is visible only inside its context.
        let _context = runtime.enter();
        let mut detector = StallDetector::new(STALL_LIMIT);
        loop {
            std::thread::sleep(CHECK_INTERVAL);
            let Some(cell) = lock().clone() else {
                detector.reset();
                continue;
            };
            if let Some(stall) = detector.observe(rt::Instant::now(), Instant::now()) {
                report(&format!(
                    "{cell}: model time did not advance for {}s of wall time",
                    stall.as_secs()
                ));
                std::process::exit(1);
            }
        }
    });
}

/// Finds a model clock that does not advance for a limit of wall time.
struct StallDetector<M> {
    limit: Duration,
    /// The last model instant, and the wall time when it was first seen.
    last: Option<(M, Instant)>,
}

impl<M: PartialEq> StallDetector<M> {
    fn new(limit: Duration) -> Self {
        Self { limit, last: None }
    }

    /// Records that the model clock reads `model` at the wall time `wall`.
    /// Returns the wall time since the last advance, if it is the limit or
    /// more.
    fn observe(&mut self, model: M, wall: Instant) -> Option<Duration> {
        match &self.last {
            Some((last, since)) if *last == model => {
                let stall = wall.saturating_duration_since(*since);
                (stall >= self.limit).then_some(stall)
            }
            _ => {
                self.last = Some((model, wall));
                None
            }
        }
    }

    /// Forgets the last model instant.
    fn reset(&mut self) {
        self.last = None;
    }
}

fn lock() -> std::sync::MutexGuard<'static, Option<String>> {
    CELL.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(60);

    fn at(start: Instant, secs: u64) -> Instant {
        start + Duration::from_secs(secs)
    }

    #[test]
    fn stopped_model_time_fails_at_the_limit() {
        let start = Instant::now();
        let mut detector = StallDetector::new(LIMIT);
        assert_eq!(detector.observe(7, start), None);
        assert_eq!(detector.observe(7, at(start, 59)), None);
        assert_eq!(detector.observe(7, at(start, 60)), Some(LIMIT));
    }

    #[test]
    fn an_advance_starts_the_wall_time_again() {
        let start = Instant::now();
        let mut detector = StallDetector::new(LIMIT);
        detector.observe(1, start);
        detector.observe(2, at(start, 59));
        assert_eq!(detector.observe(2, at(start, 118)), None);
        assert_eq!(
            detector.observe(2, at(start, 119)),
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn a_reset_forgets_the_last_instant() {
        let start = Instant::now();
        let mut detector = StallDetector::new(LIMIT);
        detector.observe(1, start);
        detector.reset();
        assert_eq!(detector.observe(1, at(start, 60)), None);
        assert_eq!(detector.observe(1, at(start, 119)), None);
    }
}
