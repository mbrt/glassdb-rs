//! Transaction engine. Ported from the Go `internal/trans` package: the commit
//! algorithm, distributed locking, lifecycle monitor, read path, and GC.

mod access;
mod algo;
mod collection_catalog;
mod collection_commit;
mod collection_coordination;
mod collections;
mod engine;
mod error;
mod gc;
mod key_resolver;
mod key_state_resolver;
mod leaf_coord;
mod monitor;
mod node_locking;
mod reader;
mod split;
mod tlocker;
mod wound_wait;

pub use access::{
    AccessSet, ReadAccess, ReadEvidence, ScanAccess, ScanMutation, ScanRange, WriteAccess,
};
pub use algo::{BodyDecision, DirectCommitStats};
pub use collections::{
    CatalogAccesses, CollectionChange, CollectionOp, DirectoryRead, DirectoryReadKind,
    DirectorySnapshot,
};
pub use engine::{Engine, EngineConfig, EngineDiagnostics, EngineStats, EngineTransaction};
pub use error::TransError;
pub use key_resolver::ScanResult;
pub use leaf_coord::LeafCoordinatorStats;
pub use monitor::ProtocolTiming;
pub use reader::{ReadOutcome, ReadValue};
pub use split::{InlinePressureStats, SplitterStats};
pub use tlocker::LockerStats;
