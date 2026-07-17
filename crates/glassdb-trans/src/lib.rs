//! Transaction engine. Ported from the Go `internal/trans` package: the commit
//! algorithm, distributed locking, lifecycle monitor, read path, and GC.

mod algo;
mod error;
mod gc;
mod monitor;
mod node_locking;
mod reader;
mod resolver;
mod shard_coord;
mod split;
mod tlocker;

pub use algo::{
    Algo, Data, Handle, LeafCoverage, ReadAccess, ScanAccess, ScanMutation, ScanRange, WriteAccess,
};
pub use error::TransError;
pub use gc::Gc;
pub use monitor::Monitor;
pub use reader::{ReadOutcome, ReadValue, Reader};
pub use resolver::{Resolver, ScanResult};
pub use shard_coord::{ShardCoordinator, SplitHinter};
pub use split::Splitter;
pub use tlocker::{Locker, TxLockSnapshot};

// Re-exported so the public diagnostics surface does not force callers to pull
// in `glassdb-concurr` directly.
pub use glassdb_concurr::DedupKeySnapshot;
