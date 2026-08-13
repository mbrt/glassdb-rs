//! Compatibility facade for deterministic simulation execution.
//!
//! Runtime callers continue to use this module while the simulation policies
//! and executor kernel live under [`crate::sim`].

pub(crate) use crate::sim::executor::{DetYield, det_sleep, det_spawn, fill_random, now_nanos};
// Preserve crate-visible facade paths even when no current sibling imports them.
#[allow(unused_imports)]
pub(crate) use crate::sim::executor::{DetSleep, Handle, current};
pub use crate::sim::executor::{TaskId, block_on_with, in_sim};
pub use crate::sim::scheduler::{PctScheduler, RandomScheduler, Scheduler, TapeScheduler};
