//! How the benchmark process advances model time.

use std::error::Error;

use clap::ValueEnum;
use glassdb_concurr::rt;
use tokio::runtime::{Builder, Runtime};

use crate::backend;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum TimeMode {
    /// Model time follows wall time, compressed by `--delay-scale` for the
    /// synthetic backends. Workers run on all cores, so the host CPU is part
    /// of what a run measures.
    Real,
    /// Model time jumps over the waits of an idle process. Workers share one
    /// thread and CPU work takes no model time, so a run of the memory backend
    /// does not depend on the host.
    Virtual,
}

impl TimeMode {
    /// Returns the name that the result records.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Virtual => "virtual",
        }
    }

    /// Installs the process-wide model time, and returns its speedup.
    ///
    /// The speedup is 1 for virtual time: the paused clock of the runtime
    /// already is the model clock, so `rt` must only anchor wall timestamps to
    /// it.
    pub(crate) fn install(self, backend: &backend::Options) -> Result<f64, Box<dyn Error>> {
        let speedup = match self {
            Self::Real => backend.model_time_speedup()?,
            Self::Virtual => {
                backend.check_virtual_time()?;
                Some(1.0)
            }
        };
        if let Some(speedup) = speedup {
            rt::set_model_time_speedup(speedup)?;
        }
        Ok(speedup.unwrap_or(1.0))
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
