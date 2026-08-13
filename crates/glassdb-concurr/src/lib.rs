//! Concurrency utilities: background task management, mergeable work
//! deduplication, and retry-with-backoff. Cancellation throughout is by
//! future-drop (`tokio::time::timeout`, `select!`, `JoinHandle::abort`);
//! [`tokio_util::sync::CancellationToken`] is the small wakeup primitive used
//! wherever an outside caller needs to drop a specific in-flight future (sim
//! `JoinHandle::abort`, `Dedup::close`, simulation-harness crash nemesis).
mod background;
mod dedup;
pub mod entropy;
#[cfg(sim)]
pub mod exec;
mod retry;
mod rng;
pub mod rt;
pub mod shard;
mod tape;

pub use background::Background;
pub use dedup::{
    BatchHandle, Dedup, DedupError, DedupKeySnapshot, DedupStats, MergeRequest, Worker,
};
pub use retry::{Backoff, RetryConfig};
pub use rng::Rng;
pub use tape::Tape;
