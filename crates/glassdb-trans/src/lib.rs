//! Transaction engine. Ported from the Go `internal/trans` package: the commit
//! algorithm, distributed locking, lifecycle monitor, read path, and GC.

mod access;
mod algo;
mod collections;
mod directory_locker;
mod engine;
mod error;
mod gc;
mod monitor;
mod node_locking;
mod reader;
mod resolver;
mod shard_coord;
mod split;
mod tlocker;
mod wound_wait;

pub use access::{
    Data, LeafCoverage, ReadAccess, ScanAccess, ScanMutation, ScanRange, WriteAccess,
};
pub use algo::DirectCommitStats;
pub use collections::{
    CollectionChange, CollectionData, CollectionOp, DirectoryRead, DirectoryReadKind,
    DirectorySnapshot,
};
pub use engine::{Engine, EngineConfig, EngineDiagnostics, EngineStats, EngineTransaction};
pub use error::TransError;
pub use monitor::ProtocolTiming;
pub use reader::{ReadOutcome, ReadValue};
pub use resolver::ScanResult;
pub use shard_coord::ShardCoordinatorStats;
pub use split::{InlinePressureStats, SplitterStats};
pub use tlocker::{HeldLeafSnapshot, LockerStats, TxLockSnapshot};

// Re-exported so the public diagnostics surface does not force callers to pull
// in `glassdb-concurr` directly.
pub use glassdb_concurr::DedupKeySnapshot;
