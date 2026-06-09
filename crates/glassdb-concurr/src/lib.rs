//! Concurrency utilities: background task management, mergeable work
//! deduplication, and retry-with-backoff. `CancelToken` is re-exported only
//! because it remains an implementation detail of `Dedup`
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
