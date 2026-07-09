//! Storage and caching layers: a byte-weighted LRU cache shared by two facades
//! — a writer-keyed [`ValueCache`] for user values and a backend-version-keyed,
//! read/write-through [`ObjectCache`] for coordination objects — plus the shard
//! / root coordination store and transaction-log persistence.

pub mod cache;
mod directory;
mod entry;
mod error;
mod lock;
mod node;
mod object_cache;
mod root;
mod shard;
mod shardstore;
mod tlogger;
pub mod txobject;
mod value_cache;
mod version;

pub use cache::{Cache, Weighable};
pub use directory::{Directory, LeafGroup, LeafLocator};
pub use entry::SharedCache;
pub use error::StorageError;
pub use lock::LockType;
pub use node::{IndexNode, Node, NodeBody, NodeToken};
pub use object_cache::{Freshness, ObjectCache, ObjectRead};
pub use root::CollectionRoot;
pub use shard::{Shard, ShardEntry};
pub use shardstore::{LeafKind, LoadedLeaf, ShardStore};
pub use tlogger::{PathLock, TLogger, TValue, TxCommitStatus, TxLog, TxStatus, TxWrite};
pub use value_cache::{MAX_STALENESS, ValueCache, ValueRead};
pub use version::Version;
