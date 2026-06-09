//! Decorators for [`Backend`](crate::Backend) implementations: simulated
//! latency, deterministic scheduling, and operation logging. Ported from the
//! Go `backend/middleware` package.

mod delay;
mod fault;
mod logger;
mod recording;
mod scheduled;

pub use delay::{DelayBackend, DelayOptions, Latency, gcs_delays, s3_delays};
pub use fault::{FaultBackend, FaultOptions};
pub use logger::BackendLogger;
pub use recording::{OpLog, OpRecord, RecordingBackend, first_divergence};
pub use scheduled::{ScheduledBackend, Scheduler};
