//! Transaction engine. Ported from the Go `internal/trans` package: the commit
//! algorithm, distributed locking, lifecycle monitor, read path, and GC.

mod algo;
mod error;
mod gc;
mod monitor;
mod reader;
mod resolver;
mod shard_coord;
mod tlocker;

pub use algo::{Algo, Data, Handle, ReadAccess, ReadVersion, WriteAccess};
pub use error::TransError;
pub use gc::Gc;
pub use monitor::Monitor;
pub use reader::{ReadValue, Reader};
pub use resolver::Resolver;
pub use shard_coord::ShardCoordinator;
pub use tlocker::{Locker, TxLockSnapshot};

// Re-exported so the public diagnostics surface does not force callers to pull
// in `glassdb-concurr` directly.
pub use glassdb_concurr::DedupKeySnapshot;
