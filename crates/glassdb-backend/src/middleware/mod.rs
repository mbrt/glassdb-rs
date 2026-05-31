//! Decorators for [`Backend`](crate::Backend) implementations: simulated
//! latency, deterministic scheduling, and operation logging. Ported from the
//! Go `backend/middleware` package.

mod delay;
mod logger;
mod scheduled;

pub use delay::{gcs_delays, s3_delays, DelayBackend, DelayOptions, Latency};
pub use logger::BackendLogger;
pub use scheduled::{ScheduledBackend, Scheduler};
