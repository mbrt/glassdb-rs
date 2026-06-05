//! Concurrency utilities ported from the Go `internal/concurr` package:
//! a cancellation context, background task management, bounded fan-out,
//! mergeable work deduplication, retry-with-backoff, and an infinite-capacity
//! channel.

mod background;
mod cancel;
mod channel;
mod clock;
pub mod ctx;
mod dedup;
#[cfg(sim)]
mod exec;
mod fanout;
mod retry;
mod rng;
pub mod rt;
pub mod shard;
mod tape;

pub use background::Background;
pub use cancel::CancelToken;
pub use channel::make_chan_inf_cap;
pub use clock::Clock;
pub use ctx::{Cancelled, Ctx};
pub use dedup::{await_signal, Controller, Dedup, DedupError, DedupWorker, MergeRequest};
pub use fanout::Fanout;
pub use retry::{retry, retry_with_backoff, RetryErr};
pub use rng::Rng;
pub use shard::Sharded;
pub use tape::Tape;
