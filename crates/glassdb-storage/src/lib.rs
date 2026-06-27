//! Storage and caching layers. Ported from the Go `internal/storage` and
//! `internal/cache` packages: a byte-weighted LRU cache, version tracking, a
//! staleness-aware local cache, read/write-through global storage, lock-state
//! encoding with the pure lock-transition logic, and transaction-log
//! persistence.

pub mod cache;
mod error;
mod global;
mod local;
mod locker;
mod root;
mod shard;
mod shardstore;
mod tlogger;
pub mod txobject;
mod version;

pub use cache::{Cache, Weighable};
pub use error::StorageError;
pub use global::{Global, GlobalRead};
pub use local::{Local, LocalMetadata, LocalRead, MAX_STALENESS};
pub use locker::{
    LockInfo, LockOps, LockRequest, LockType, LockUpdate, Locker, TValue, TxPathState,
    compute_lock_update, last_writer_from_tags, tags_lock_info,
};
pub use root::CollectionRoot;
pub use shard::{Shard, ShardEntry};
pub use shardstore::ShardStore;
pub use tlogger::{PathLock, TLogger, TxCommitStatus, TxLog, TxStatus, TxWrite};
pub use version::{Version, version_from_meta};
