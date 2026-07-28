//! Decoded, byte-bounded physical-object storage plus shard/root coordination,
//! transaction-log persistence, and structural split recovery records.

pub mod cache;
mod cache_stats;
mod cached_store;
mod collection_store;
mod directory;
mod disk_cache;
mod error;
mod inline;
mod lock;
mod node;
mod shard;
mod shard_store;
mod structlog;
mod timeline;
mod tlogger;
pub mod txobject;
mod version;

/// Persistent-cache media and harnesses for deterministic simulation.
#[cfg(feature = "sim")]
pub mod sim {
    pub use crate::disk_cache::sim_media::{MediaFaultProfile, MediaPause, SimMedia};

    /// Isolated persistent-cache deterministic simulation.
    #[cfg(sim)]
    pub mod disk_cache {
        pub use crate::disk_cache::sim_harness::{
            DiskCacheEvent, record_disk_cache_input, replay_disk_cache_input,
        };
    }
}

pub use cache::{Cache, Weighable};
pub use cache_stats::CacheStats;
pub use cached_store::{
    CachedStore, CasResult, Observation, ObservationCheck, Requirement, Revision,
};
pub use collection_store::{CollectionRecord, CollectionStore};
pub use directory::{Directory, LeafGroup, LeafLocator};
pub use disk_cache::{
    OpenedPersistentCache, PersistentCache, PersistentCacheConfig, PersistentCacheMedia,
};
pub use error::StorageError;
pub use inline::InlinePolicy;
pub use lock::LockType;
pub use node::{IndexNode, Node, NodeBody, NodeLock, NodeLocks, NodeToken, SplitPolicy};
pub use shard::{CurrentState, Shard, ShardEntry};
pub use shard_store::{LeafObservation, LeafObservationCheck, LoadedLeaf, ShardStore};
pub use structlog::{StructuralLog, StructuralLogPhase};
pub use timeline::{SequencePoint, Timeline};
pub use tlogger::{
    TLogger, TValue, TxCollectionChange, TxCollectionOp, TxCommitStatus, TxListPage, TxLock, TxLog,
    TxStatus, TxWrite,
};
pub use version::Version;
