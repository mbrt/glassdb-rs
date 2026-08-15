//! Decoded, byte-bounded physical-object storage plus shard/root coordination,
//! transaction-log persistence, and structural split recovery records.

pub mod cache;
mod cache_stats;
mod cached_store;
mod collection_store;
mod disk_cache;
mod error;
mod inline;
mod lock;
mod node;
mod node_store;
mod shard;
mod structlog;
mod structural_log_store;
mod timeline;
pub mod transaction;
mod tree_router;
pub mod txobject;
mod version;
mod wire_size;

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

pub use cache_stats::CacheStats;
pub use cached_store::{
    CachedStore, CasResult, Observation, ObservationCheck, Requirement, Revision,
};
pub use collection_store::{CollectionRecord, CollectionStore};
pub use disk_cache::{
    OpenedPersistentCache, PersistentCache, PersistentCacheConfig, PersistentCacheMedia,
};
pub use error::StorageError;
pub use inline::InlinePolicy;
pub use lock::{EntryLockState, ExclusiveGate, LockType, SharedExclusiveLock};
pub use node::{
    IndexNode, InvalidSplitPolicy, Node, NodeBody, NodeLocks, NodeToken, SplitPolicy,
    SplitPolicyBuilder,
};
pub use node_store::{LeafEdit, LeafObservation, LeafObservationCheck, LoadedLeaf, NodeStore};
pub use shard::{CurrentState, Shard, ShardEntry};
pub use structlog::{StructuralLog, StructuralLogPhase};
pub use structural_log_store::StructuralLogStore;
pub use timeline::{SequencePoint, Timeline};
pub use tree_router::{LeafGroup, LeafLocator, TreeRouter};
pub use version::Version;
