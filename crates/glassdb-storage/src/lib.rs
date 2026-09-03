//! Decoded, byte-bounded physical-object storage plus leaf/root coordination,
//! transaction-log persistence, and structural intents for split recovery.

pub mod cache;
mod cache_stats;
mod cached_store;
mod collection_store;
mod disk_cache;
mod error;
mod inline;
mod leaf;
mod lock;
mod node;
mod node_store;
mod structural_intent;
mod structural_intent_store;
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
pub use leaf::{CurrentState, LeafBody, LeafEntry};
pub use lock::{EntryLockState, ExclusiveGate, LockType, SharedExclusiveLock};
pub use node::{
    IndexNode, InvalidSplitPolicy, Node, NodeBody, NodeLocks, NodeToken, SplitPolicy,
    SplitPolicyBuilder,
};
pub use node_store::{LeafEdit, LeafObservation, LeafObservationCheck, LoadedLeaf, NodeStore};
pub use structural_intent::{StructuralIntent, StructuralIntentPhase};
pub use structural_intent_store::StructuralIntentStore;
pub use timeline::{SequencePoint, Timeline};
pub use tree_router::{RoutedLeaf, RoutedLeafGroup, TreeRouter};
pub use version::Version;
