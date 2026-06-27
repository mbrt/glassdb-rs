//! Storage and caching layers. Ported from the Go `internal/storage` and
//! `internal/cache` packages: a byte-weighted LRU cache, version tracking, a
//! staleness-aware local cache, read/write-through global storage, the shard /
//! root coordination objects, and transaction-log persistence.

pub mod cache;
mod error;
mod global;
mod local;
mod lock;
mod root;
mod shard;
mod shardstore;
mod tlogger;
pub mod txobject;
mod version;

pub use cache::{Cache, Weighable};
pub use error::StorageError;
pub use global::{Global, GlobalRead};
pub use local::{Local, LocalRead, MAX_STALENESS};
pub use lock::LockType;
pub use root::CollectionRoot;
pub use shard::{Shard, ShardEntry};
pub use shardstore::ShardStore;
pub use tlogger::{PathLock, TLogger, TValue, TxCommitStatus, TxLog, TxStatus, TxWrite};
pub use version::Version;
