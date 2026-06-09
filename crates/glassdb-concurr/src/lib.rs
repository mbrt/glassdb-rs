//! Concurrency utilities: background task management, mergeable work
//! deduplication, and retry-with-backoff. The `CancelToken` re-exported here
//! is an implementation detail of `Background` and `Dedup` — it surfaces only
//! because closures passed to them observe the shutdown signal.

mod background;
mod cancel;
mod clock;
mod dedup;
#[cfg(sim)]
mod exec;
mod retry;
mod rng;
pub mod rt;
pub mod shard;
mod tape;

pub use background::Background;
pub use cancel::CancelToken;
pub use clock::Clock;
pub use dedup::{BatchHandle, Dedup, DedupError, DedupKeySnapshot, MergeRequest, Worker};
pub use retry::{Backoff, RetryConfig, RetryErr, retry, retry_with_backoff};
pub use rng::Rng;
pub use shard::Sharded;
pub use tape::Tape;
