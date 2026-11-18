//! How the benchmark process advances model time.

use std::error::Error;
use std::time::Duration;

use clap::ValueEnum;
use glassdb_concurr::rt;
use tokio::runtime::{Builder, Runtime};

use crate::backend;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum TimeMode {
    /// Model time is wall time. Workers run on all cores, so the host CPU is
    /// part of what a run measures.
    Real,
    /// Model time jumps over the waits of an idle process. Workers share one
    /// thread and CPU work takes no model time, so a run of the memory backend
    /// does not depend on the host.
    Virtual,
}

/// The model time that each transaction spends in virtual time, in place of
/// its CPU work. It is close to the mean CPU time of a transaction of the
/// `stale-reads` grid with the memory backend: about 0.2 ms, measured as the
/// wall time of a measurement in virtual time over its transactions. A kind
/// with more CPU work, such as a scan of a whole collection, takes longer in
/// real time. It need not be exact,
/// only not zero, so that a transaction with no backend operation still
/// advances model time.
const VIRTUAL_TRANSACTION_CPU: Duration = Duration::from_micros(200);

impl TimeMode {
    /// Returns the model time that a worker spends after each transaction, in
    /// place of the CPU work that takes no model time. Real time measures the
    /// CPU work itself, so it adds none.
    pub(crate) fn transaction_cpu(self) -> Duration {
        match self {
            Self::Real => Duration::ZERO,
            Self::Virtual => VIRTUAL_TRANSACTION_CPU,
        }
    }

    /// Returns the name that the result records.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Virtual => "virtual",
        }
    }

    /// Installs the process-wide model time.
    ///
    /// In real time, model time runs at the speed of wall time. For a
    /// synthetic backend, the speedup of 1 also anchors wall timestamps to
    /// the monotonic clock, so a step of the system clock does not move them.
    /// Real providers stay on live wall time, because their clocks cannot
    /// share this anchor.
    pub(crate) fn install(self, backend: &backend::Options) -> Result<(), Box<dyn Error>> {
        self.check(backend)?;
        match self {
            Self::Real if backend.is_synthetic() => rt::set_model_time_speedup(1.0)?,
            Self::Real => {}
            Self::Virtual => rt::set_paused_model_time()?,
        }
        Ok(())
    }

    /// Checks that the mode can run with `backend`.
    pub(crate) fn check(self, backend: &backend::Options) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Real => Ok(()),
            Self::Virtual => backend.check_virtual_time(),
        }
    }

    /// Builds the runtime that drives the process.
    pub(crate) fn runtime(self) -> std::io::Result<Runtime> {
        match self {
            Self::Real => Builder::new_multi_thread().enable_all().build(),
            // Time advances only when no task can run. A task that waits for
            // an operating-system event, such as a socket, would see time jump
            // while the event is still pending.
            Self::Virtual => Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build(),
        }
    }
}
