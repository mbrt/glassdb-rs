//! Decoded, byte-bounded physical-object storage plus shard/root coordination,
//! transaction-log persistence, and structural split recovery records.

pub mod cache;
mod cache_stats;
mod cached_store;
mod directory;
mod disk_cache;
mod error;
mod lock;
mod node;
mod root;
mod shard;
mod shardstore;
mod structlog;
mod timeline;
mod tlogger;
pub mod txobject;
mod version;

pub use cache::{Cache, Weighable};
pub use cache_stats::CacheStats;
pub use cached_store::{
    CachedStore, CasResult, Observation, ObservationCheck, Requirement, Revision,
};
pub use directory::{Directory, LeafGroup, LeafLocator};
pub use disk_cache::{OpenedPersistentCache, PersistentCache, PersistentCacheConfig};
pub use error::StorageError;
pub use lock::LockType;
pub use node::{IndexNode, Node, NodeBody, NodeLock, NodeLocks, NodeToken, SplitPolicy};
pub use root::CollectionRoot;
pub use shard::{Shard, ShardEntry};
pub use shardstore::{LeafKind, LeafObservation, LeafObservationCheck, LoadedLeaf, ShardStore};
pub use structlog::StructuralLog;
pub use timeline::{SequencePoint, Timeline};
pub use tlogger::{TLogger, TValue, TxCommitStatus, TxListPage, TxLock, TxLog, TxStatus, TxWrite};
pub use version::Version;
