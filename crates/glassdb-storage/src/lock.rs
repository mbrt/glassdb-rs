//! The lock-state value type shared across the storage and transaction layers.
//!
//! The v1 tag-based lock encoding and the pure lock-transition logic
//! (`compute_lock_update` & co.) are gone: the v2 engine coordinates with
//! content CAS on shard/root objects (`glassdb-trans::tlocker`), where the lock
//! table lives in the object body, not in tags. This module keeps the
//! storage-domain [`LockType`] that mirrors the protobuf `lock::LockType`, plus
//! the node-level [`NodeLock`] of ADR-032.

use glassdb_data::TxId;

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

/// A node-level read/write lock (ADR-032): the **structure** or **membership**
/// lock that lives in a B-link node object beside the per-key entry locks. A
/// read lock is shared (many holders); a write lock is exclusive (one holder).
/// An empty value (no holders, [`LockType::None`]) means unlocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLock {
    /// The strength of the lock; [`LockType::None`] when unlocked.
    pub lock_type: LockType,
    /// Transactions holding the lock (more than one only for a read lock).
    pub holders: Vec<TxId>,
}

impl Default for NodeLock {
    fn default() -> Self {
        NodeLock {
            lock_type: LockType::None,
            holders: Vec::new(),
        }
    }
}

impl NodeLock {
    /// Reports whether the lock records nothing worth keeping: no holder and no
    /// meaningful strength. Such a value is indistinguishable from an absent
    /// lock, so the node encoding omits it to stay canonical.
    pub fn is_empty(&self) -> bool {
        self.holders.is_empty() && matches!(self.lock_type, LockType::None | LockType::Unknown)
    }

    /// Reports whether `id` holds the lock.
    pub fn holds(&self, id: &TxId) -> bool {
        self.holders.contains(id)
    }

    /// The holders other than `id`: the conflicting parties a would-be holder
    /// must reconcile with (wound-wait) before it can take the lock (ADR-032).
    pub fn other_holders<'a>(&'a self, id: &'a TxId) -> impl Iterator<Item = &'a TxId> {
        self.holders.iter().filter(move |h| *h != id)
    }

    /// Adds `id` as a shared **read** holder, idempotently. A read lock coexists
    /// with other read holders; the caller is responsible for having reconciled
    /// any conflicting **write** holder first (ADR-032).
    pub fn acquire_read(&mut self, id: &TxId) {
        if !self.holds(id) {
            self.holders.push(id.clone());
        }
        // A read acquire never weakens an existing write hold by the same tx.
        if self.lock_type != LockType::Write {
            self.lock_type = LockType::Read;
        }
    }

    /// Installs `id` as the sole **write** holder, replacing any prior holders.
    /// The caller must have reconciled every conflicting holder first (ADR-032).
    pub fn acquire_write(&mut self, id: &TxId) {
        self.holders = vec![id.clone()];
        self.lock_type = LockType::Write;
    }

    /// Drops `id` from the holder set, resetting the strength to
    /// [`LockType::None`] once the last holder leaves.
    pub fn release(&mut self, id: &TxId) {
        self.holders.retain(|h| h != id);
        if self.holders.is_empty() {
            self.lock_type = LockType::None;
        }
    }
}

/// The full node-level lock state of a B-link node (ADR-032): the structure and
/// membership locks plus the membership version. Threaded through a coordination
/// round beside the per-key entries so a mutation, scan, or split can install or
/// release its node-level holds in the same CAS that touches the entries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeLocks {
    /// The structure lock: guards the node's shape.
    pub structure: NodeLock,
    /// The membership lock: guards a leaf's live key set.
    pub membership: NodeLock,
    /// The leaf's membership version, the OCC fast-path token for scans.
    pub membership_version: u64,
}

impl NodeLocks {
    /// Installs `id`'s exclusive **membership-write** hold (create/delete) and
    /// bumps the membership version in one step (ADR-032), so a caller can never
    /// take the membership-write lock without recording the activity the OCC
    /// fast path relies on. The caller must have reconciled every conflicting
    /// holder first (via [`reconcile_node_lock`](crate::lock)-style wound-wait).
    pub fn acquire_membership_write(&mut self, id: &TxId) {
        self.membership.acquire_write(id);
        self.bump_membership_version();
    }

    /// Drops every node-level hold `id` has — the structure lock and, for a
    /// create/delete, the membership lock — bumping the membership version iff a
    /// membership hold was actually released (ADR-032), so releasing structure
    /// alone (a split, a plain mutation) never disturbs a scan's OCC token.
    /// Returns whether anything changed, so the caller can skip a CAS when this
    /// transaction held no node lock.
    pub fn release(&mut self, id: &TxId) -> bool {
        let mut changed = false;
        if self.structure.holds(id) {
            self.structure.release(id);
            changed = true;
        }
        if self.membership.holds(id) {
            self.membership.release(id);
            self.bump_membership_version();
            changed = true;
        }
        changed
    }

    /// Bumps the membership version, the effect of membership-write activity
    /// (ADR-032). Private: only the membership-write operations above invoke it,
    /// so the version can never be advanced out of step with the lock it tracks.
    fn bump_membership_version(&mut self) {
        self.membership_version = self.membership_version.wrapping_add(1);
    }
}
