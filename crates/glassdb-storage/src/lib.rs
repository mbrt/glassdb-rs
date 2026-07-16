//! Storage and caching layers: a decoded, path-keyed object cache
//! ([`CachedStore`]) with bounded-freshness validation (ADR-036), fronted by
//! the backend-version-keyed, read/write-through [`ObjectCache`] facade for
//! coordination objects — plus the shard / root coordination store,
//! transaction-log persistence, and structural split recovery records.

pub mod cache;
mod cached_store;
mod directory;
mod entry;
mod error;
mod lock;
mod node;
mod object_cache;
mod root;
mod shard;
mod shardstore;
mod structlog;
mod tlogger;
pub mod txobject;
mod version;

pub use cache::{Cache, Weighable};
pub use cached_store::{
    CachedStore, CasResult, Codec, Observation, Requirement, Revision, Validated, ValidationTime,
};
pub use directory::{Directory, LeafGroup, LeafLocator};
pub use entry::SharedCache;
pub use error::StorageError;
pub use lock::LockType;
pub use node::{IndexNode, Node, NodeBody, NodeLock, NodeLocks, NodeToken, SplitPolicy};
pub use object_cache::{ObjectCache, ObjectRead};
pub use root::CollectionRoot;
pub use shard::{Shard, ShardEntry};
pub use shardstore::{LeafKind, LoadedLeaf, ShardStore};
pub use structlog::StructuralLog;
pub use tlogger::{
    LockScope, PathLock, TLogger, TValue, TxCommitStatus, TxListPage, TxLog, TxStatus, TxWrite,
};
pub use version::Version;
