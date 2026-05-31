//! Concurrency utilities ported from the Go `internal/concurr` package:
//! a cancellation context, background task management, bounded fan-out,
//! mergeable work deduplication, retry-with-backoff, and an infinite-capacity
//! channel.

mod background;
mod channel;
mod clock;
pub mod ctx;
mod dedup;
mod fanout;
mod retry;
pub mod shard;

pub use background::Background;
pub use channel::make_chan_inf_cap;
pub use clock::Clock;
pub use ctx::{Cancelled, Ctx};
pub use dedup::{await_signal, Controller, Dedup, DedupError, DedupWorker, MergeRequest};
pub use fanout::Fanout;
pub use retry::{retry, retry_with_backoff, RetryErr};
pub use shard::Sharded;
