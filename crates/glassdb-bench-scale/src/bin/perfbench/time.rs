//! How the benchmark process advances model time.

use std::error::Error;
use std::time::Duration;

use clap::ValueEnum;
use glassdb_concurr::rt;
use rand::{Rng, RngExt};
use tokio::runtime::{Builder, Runtime};

use crate::{backend, stall};

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

/// The model time of one unit of CPU work in virtual time: about the CPU time
/// of one key that a transaction reads with warm caches and the memory
/// backend. Each kind gives the units of one transaction
/// ([`Kind::cpu_units`](crate::runner::kinds::Kind::cpu_units)). The value
/// is a first-order approximation, measured as the wall time of a measurement
/// in virtual time over its transactions, with one worker of each kind.
const VIRTUAL_CPU_UNIT: Duration = Duration::from_micros(5);

/// The largest change of a CPU charge from its mean, as a fraction of the
/// mean, with `--latency-jitter`. It is a choice, near the ratio of 1.1 to 1.6
/// between the p90 and the p50 latency of reads with no backend operation in
/// real time.
const CPU_JITTER: f64 = 0.15;

/// The model time that a worker spends after each transaction, in place of
/// the CPU work that takes no model time. It is in the measured latency.
#[derive(Clone, Copy)]
pub(crate) struct TransactionCpu {
    unit: Duration,
    jitter: bool,
}

impl TransactionCpu {
    /// Adds no model time.
    #[cfg(test)]
    pub(crate) const ZERO: Self = Self {
        unit: Duration::ZERO,
        jitter: false,
    };

    /// Returns the model time of a transaction of `units` units of CPU work.
    /// With jitter, it is uniform within [`CPU_JITTER`] of its mean. A
    /// transaction costs at least one unit, so that a transaction with no
    /// backend operation still advances model time.
    pub(crate) fn charge(self, units: usize, rng: &mut impl Rng) -> Duration {
        let mean = self.unit * u32::try_from(units.max(1)).unwrap_or(u32::MAX);
        if !self.jitter || mean.is_zero() {
            return mean;
        }
        mean.mul_f64(rng.random_range(1.0 - CPU_JITTER..=1.0 + CPU_JITTER))
    }
}

impl TimeMode {
    /// Returns the model time that a worker spends after each transaction, in
    /// place of the CPU work that takes no model time. Real time measures the
    /// CPU work itself, so it adds none. `jitter` follows `--latency-jitter`.
    pub(crate) fn transaction_cpu(self, jitter: bool) -> TransactionCpu {
        let unit = match self {
            Self::Real => Duration::ZERO,
            Self::Virtual => VIRTUAL_CPU_UNIT,
        };
        TransactionCpu { unit, jitter }
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

    /// Starts the watchdog of `runtime` in virtual time, where a task that
    /// runs in a loop stops model time. It calls `report` with the error
    /// before it ends the process. In real time, model time is wall time, so
    /// the maximum duration of a measurement ends such a loop.
    pub(crate) fn watch(self, runtime: &Runtime, report: impl Fn(&str) + Send + 'static) {
        if self == Self::Virtual {
            stall::watch(runtime.handle().clone(), report);
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

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    #[test]
    fn a_charge_without_jitter_is_exact() {
        let cpu = TimeMode::Virtual.transaction_cpu(false);
        let mut rng = StdRng::seed_from_u64(1);
        for _ in 0..100 {
            assert_eq!(cpu.charge(3, &mut rng), VIRTUAL_CPU_UNIT * 3);
        }
    }

    #[test]
    fn a_charge_with_jitter_varies_around_its_mean() {
        let cpu = TimeMode::Virtual.transaction_cpu(true);
        let mut rng = StdRng::seed_from_u64(1);
        let mean = VIRTUAL_CPU_UNIT * 16;
        let charges: Vec<Duration> = (0..1000).map(|_| cpu.charge(16, &mut rng)).collect();
        for charge in &charges {
            assert!(*charge >= mean.mul_f64(1.0 - CPU_JITTER), "{charge:?}");
            assert!(*charge <= mean.mul_f64(1.0 + CPU_JITTER), "{charge:?}");
        }
        assert!(charges.iter().any(|charge| *charge != charges[0]));
        let average = charges.iter().sum::<Duration>() / 1000;
        assert!(average.abs_diff(mean) < mean / 50, "{average:?}");
    }

    #[test]
    fn a_transaction_costs_at_least_one_unit() {
        let cpu = TimeMode::Virtual.transaction_cpu(false);
        assert_eq!(
            cpu.charge(0, &mut StdRng::seed_from_u64(1)),
            VIRTUAL_CPU_UNIT
        );
    }

    #[test]
    fn real_time_charges_nothing() {
        let cpu = TimeMode::Real.transaction_cpu(true);
        assert_eq!(
            cpu.charge(16, &mut StdRng::seed_from_u64(1)),
            Duration::ZERO
        );
    }
}
