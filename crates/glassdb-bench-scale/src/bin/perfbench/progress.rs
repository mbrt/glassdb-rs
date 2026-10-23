//! An optional progress bar on stderr.
//!
//! The bar shows how many cells of the experiment are done. A cell counts as
//! partly done while it runs, by the elapsed part of its expected model time.
//! The expected time is an estimate, so the bar can move faster or slower than
//! the clock, and the remaining time is only a guide.

use std::fmt::{self, Write as _};
use std::io::Write as _;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use glassdb_concurr::rt;

const BAR_WIDTH: usize = 30;

/// The wall time between two updates of the bar. More frequent updates make
/// the remaining time hard to read.
const REDRAW_INTERVAL: Duration = Duration::from_secs(1);

static STATE: Mutex<Option<State>> = Mutex::new(None);

struct State {
    started: Instant,
    total_cells: usize,
    done_cells: usize,
    /// The part of the current cell that is done, from 0 to 1.
    cell_fraction: f64,
    /// Whether the bar is on screen now.
    drawn: bool,
    /// Whether the first stage of a cell started. The setup before it logs its
    /// own lines, so the bar waits for that stage.
    visible: bool,
}

/// The part of a cell that one stage of it takes.
#[derive(Clone, Copy)]
pub(crate) struct Stage {
    pub(crate) started: rt::Instant,
    /// The estimated model time of the stage.
    pub(crate) expected: Duration,
    /// The fraction of the cell at the start and at the end of the stage.
    pub(crate) from: f64,
    pub(crate) to: f64,
}

impl Stage {
    /// Splits a cell in a warm-up stage and a measurement stage, by their
    /// expected model times. `warm_start` is when the warm-up starts.
    pub(crate) fn split(
        warm_start: rt::Instant,
        warm: Duration,
        measure: Duration,
    ) -> (Self, Self) {
        let total = (warm + measure).as_secs_f64();
        let share = if total > 0.0 {
            warm.as_secs_f64() / total
        } else {
            0.0
        };
        let warm_stage = Self {
            started: warm_start,
            expected: warm,
            from: 0.0,
            to: share,
        };
        let measure_stage = Self {
            // The caller sets the start when the measurement starts.
            started: warm_start,
            expected: measure,
            from: share,
            to: 1.0,
        };
        (warm_stage, measure_stage)
    }

    fn fraction(&self) -> f64 {
        let part = if self.expected.is_zero() {
            0.0
        } else {
            (self.started.elapsed().as_secs_f64() / self.expected.as_secs_f64()).min(1.0)
        };
        self.from + (self.to - self.from) * part
    }
}

/// Turns the bar on for an experiment with `total_cells` cells over all runs.
pub(crate) fn enable(total_cells: usize) {
    *lock() = Some(State {
        started: Instant::now(),
        total_cells,
        done_cells: 0,
        cell_fraction: 0.0,
        drawn: false,
        visible: false,
    });
    // The bar also moves while a cell sets up and no stage reports progress,
    // so that the elapsed time continues. The thread ends with the bar.
    std::thread::spawn(|| {
        loop {
            std::thread::sleep(REDRAW_INTERVAL);
            match lock().as_mut() {
                Some(state) => draw(state),
                None => return,
            }
        }
    });
}

/// Marks the start of a cell.
pub(crate) fn start_cell() {
    update(|state| state.cell_fraction = 0.0);
}

/// Marks the end of a cell.
pub(crate) fn finish_cell() {
    update(|state| {
        state.done_cells += 1;
        state.cell_fraction = 0.0;
    });
}

/// Records the progress of `stage` now. The bar shows it at its next update.
pub(crate) fn advance(stage: Stage) {
    if let Some(state) = lock().as_mut() {
        state.cell_fraction = stage.fraction();
        state.visible = true;
    }
}

/// Removes the bar from the screen.
pub(crate) fn finish() {
    let mut guard = lock();
    if let Some(state) = guard.as_mut() {
        clear(state);
    }
    *guard = None;
}

/// Prints a line on stderr above the bar.
pub(crate) fn message(text: fmt::Arguments<'_>) {
    let mut guard = lock();
    if let Some(state) = guard.as_mut() {
        clear(state);
    }
    eprintln!("{text}");
    if let Some(state) = guard.as_mut() {
        draw(state);
    }
}

/// Prints a line on stderr above the bar, like `eprintln!`.
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::progress::message(format_args!($($arg)*))
    };
}
pub(crate) use log;

fn update(change: impl FnOnce(&mut State)) {
    let mut guard = lock();
    if let Some(state) = guard.as_mut() {
        change(state);
        draw(state);
    }
}

fn lock() -> std::sync::MutexGuard<'static, Option<State>> {
    STATE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn clear(state: &mut State) {
    if state.drawn {
        eprint!("\r\x1b[2K");
        state.drawn = false;
    }
}

fn draw(state: &mut State) {
    if !state.visible {
        return;
    }
    let fraction = if state.total_cells == 0 {
        1.0
    } else {
        ((state.done_cells as f64 + state.cell_fraction) / state.total_cells as f64).min(1.0)
    };
    let elapsed = state.started.elapsed();
    let filled = (fraction * BAR_WIDTH as f64) as usize;
    let mut line = String::from("\r\x1b[2K[");
    for position in 0..BAR_WIDTH {
        line.push(match position {
            _ if position < filled => '=',
            _ if position == filled && filled < BAR_WIDTH => '>',
            _ => ' ',
        });
    }
    let _ = write!(
        line,
        "] {:3.0}% cell {}/{} elapsed {}",
        fraction * 100.0,
        (state.done_cells + 1).min(state.total_cells),
        state.total_cells,
        format_duration(elapsed),
    );
    if fraction > 0.01 && fraction < 1.0 {
        let remaining = elapsed.mul_f64((1.0 - fraction) / fraction);
        let _ = write!(line, " eta {}", format_duration(remaining));
    }
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(line.as_bytes());
    let _ = stderr.flush();
    state.drawn = true;
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m{:02}s", seconds / 60, seconds % 60),
        _ => format!("{}h{:02}m", seconds / 3600, seconds % 3600 / 60),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_durations() {
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(62)), "1m02s");
        assert_eq!(format_duration(Duration::from_secs(3720)), "1h02m");
    }

    #[test]
    fn stages_cover_the_cell() {
        let now = rt::Instant::now();
        let (warm, measure) = Stage::split(now, Duration::from_secs(10), Duration::from_secs(30));
        assert_eq!(warm.from, 0.0);
        assert_eq!(warm.to, 0.25);
        assert_eq!(measure.from, warm.to);
        assert_eq!(measure.to, 1.0);
    }
}
