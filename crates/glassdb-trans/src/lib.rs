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
pub use monitor::{KeyCommitStatus, Monitor, WaitTxResult};
pub use reader::{ReadValue, Reader};
pub use tlocker::{LockStats, Locker};
