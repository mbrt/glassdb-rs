//! Transaction engine. Ported from the Go `internal/trans` package: the commit
//! algorithm, distributed locking, lifecycle monitor, read path, and GC.

mod algo;
mod error;
mod gc;
mod monitor;
mod reader;
mod tlocker;

pub use algo::{Algo, Data, Handle, ReadAccess, ReadVersion, WriteAccess};
pub use error::TransError;
pub use gc::Gc;
pub use monitor::Monitor;
pub use reader::{ReadValue, Reader};
pub use tlocker::{LockStats, Locker, TxLockSnapshot};

// Re-exported so the public diagnostics surface (returned by
// `Locker::dedup_snapshot` / `Algo::diagnostics`) does not force callers to
// pull in `glassdb-concurr` directly.
pub use glassdb_concurr::DedupKeySnapshot;
