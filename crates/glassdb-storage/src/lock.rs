//! The lock-state value type shared across the storage and transaction layers.
//!
//! The v1 tag-based lock encoding and the pure lock-transition logic
//! (`compute_lock_update` & co.) are gone: the v2 engine coordinates with
//! content CAS on shard/root objects (`glassdb-trans::tlocker`), where the lock
//! table lives in the object body, not in tags. This module keeps the
//! storage-domain [`LockType`] that mirrors the protobuf `lock::LockType`.

/// The type of lock held on a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LockType {
    #[default]
    Unknown,
    None,
    Read,
    Write,
    Create,
}

impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
