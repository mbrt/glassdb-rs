//! The collection root object: in-memory view and canonical protobuf encoding
//! (ADR-018).
//!
//! The root (`{prefix}/_i`) records collection existence, the constant shard
//! count, the subcollection directory, and the membership lock that serializes
//! create/delete (write lock) against listing (read lock). Its body is the
//! compare-and-swap unit and its version is the optimistic-concurrency token for
//! the whole cross-shard membership read set, so the encoding is canonical
//! (subcollections and holder sets sorted) and golden-anchored.
//!
//! This module defines an inert data type plus encode/decode and pure
//! accessors/mutators. It does no I/O; the membership-coordination protocol is
//! added by the v2 engine (ADR-020).

use std::collections::BTreeSet;

use glassdb_data::TxId;
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::locker::LockType;

/// A decoded collection root.
///
/// Subcollection names are held in a sorted set so iteration and encoding are
/// canonical regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRoot {
    shard_count: u32,
    subcollections: BTreeSet<Vec<u8>>,
    membership_lock: LockType,
    membership_locked_by: Vec<TxId>,
}

impl CollectionRoot {
    /// Creates an empty root for a collection with `shard_count` shards, no
    /// subcollections, and no membership lock held.
    pub fn new(shard_count: u32) -> Self {
        CollectionRoot {
            shard_count,
            subcollections: BTreeSet::new(),
            membership_lock: LockType::None,
            membership_locked_by: Vec::new(),
        }
    }

    /// The shard count `C` this collection was created with.
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// The membership lock type currently held on the root.
    pub fn membership_lock(&self) -> LockType {
        self.membership_lock
    }

    /// The transactions currently holding the membership lock.
    pub fn membership_locked_by(&self) -> &[TxId] {
        &self.membership_locked_by
    }

    /// Sets the membership lock to `lock` held by `holders`. A `None` lock clears
    /// the holder set.
    pub fn set_membership_lock<I: IntoIterator<Item = TxId>>(
        &mut self,
        lock: LockType,
        holders: I,
    ) {
        self.membership_lock = lock;
        self.membership_locked_by = if lock == LockType::None {
            Vec::new()
        } else {
            holders.into_iter().collect()
        };
    }

    /// Releases the membership lock entirely.
    pub fn clear_membership_lock(&mut self) {
        self.set_membership_lock(LockType::None, std::iter::empty());
    }

    /// Reports whether subcollection `name` is in the directory.
    pub fn contains_subcollection(&self, name: &[u8]) -> bool {
        self.subcollections.contains(name)
    }

    /// Adds `name` to the subcollection directory, returning whether it was newly
    /// added.
    pub fn add_subcollection(&mut self, name: impl Into<Vec<u8>>) -> bool {
        self.subcollections.insert(name.into())
    }

    /// Removes `name` from the subcollection directory, returning whether it was
    /// present.
    pub fn remove_subcollection(&mut self, name: &[u8]) -> bool {
        self.subcollections.remove(name)
    }

    /// Iterates the subcollection names in canonical (sorted) order.
    pub fn subcollections(&self) -> impl Iterator<Item = &[u8]> {
        self.subcollections.iter().map(Vec::as_slice)
    }

    /// Encodes the root to its canonical protobuf body (the CAS unit).
    pub fn encode(&self) -> Vec<u8> {
        // The holder set is sorted so logically equal roots encode identically;
        // subcollections are already canonical via the BTreeSet.
        let mut membership_locked_by: Vec<Vec<u8>> = self
            .membership_locked_by
            .iter()
            .map(|t| t.as_bytes().to_vec())
            .collect();
        membership_locked_by.sort();
        pb::CollectionRoot {
            shard_count: self.shard_count,
            subcollections: self.subcollections.iter().cloned().collect(),
            membership_lock: lock_type_to_proto(self.membership_lock) as i32,
            membership_locked_by,
        }
        .encode_to_vec()
    }

    /// Decodes a root from its protobuf body.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::CollectionRoot::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling collection root", e))?;
        Ok(CollectionRoot {
            shard_count: raw.shard_count,
            subcollections: raw.subcollections.into_iter().collect(),
            membership_lock: lock_type_from_proto(raw.membership_lock),
            membership_locked_by: raw
                .membership_locked_by
                .into_iter()
                .map(TxId::from_bytes)
                .collect(),
        })
    }
}

fn lock_type_to_proto(t: LockType) -> pb::lock::LockType {
    match t {
        LockType::None => pb::lock::LockType::None,
        LockType::Read => pb::lock::LockType::Read,
        LockType::Write => pb::lock::LockType::Write,
        LockType::Create => pb::lock::LockType::Create,
        LockType::Unknown => pb::lock::LockType::Unknown,
    }
}

fn lock_type_from_proto(t: i32) -> LockType {
    match pb::lock::LockType::try_from(t) {
        Ok(pb::lock::LockType::None) => LockType::None,
        Ok(pb::lock::LockType::Read) => LockType::Read,
        Ok(pb::lock::LockType::Write) => LockType::Write,
        Ok(pb::lock::LockType::Create) => LockType::Create,
        _ => LockType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut root = CollectionRoot::new(1024);
        root.add_subcollection(b"users".to_vec());
        root.add_subcollection(b"settings".to_vec());
        root.set_membership_lock(LockType::Write, [TxId::from_bytes(vec![1, 2, 3, 4])]);

        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
        assert_eq!(decoded.shard_count(), 1024);
        assert_eq!(decoded.membership_lock(), LockType::Write);
    }

    #[test]
    fn empty_round_trip() {
        let root = CollectionRoot::new(1024);
        let decoded = CollectionRoot::decode(&root.encode()).unwrap();
        assert_eq!(decoded, root);
        assert_eq!(decoded.membership_lock(), LockType::None);
        assert_eq!(decoded.subcollections().count(), 0);
    }

    #[test]
    fn subcollection_directory_ops() {
        let mut root = CollectionRoot::new(8);
        assert!(root.add_subcollection(b"a".to_vec()));
        assert!(!root.add_subcollection(b"a".to_vec()));
        assert!(root.contains_subcollection(b"a"));
        assert!(!root.contains_subcollection(b"missing"));
        assert!(root.remove_subcollection(b"a"));
        assert!(!root.remove_subcollection(b"a"));
    }

    #[test]
    fn subcollections_iterate_sorted() {
        let mut root = CollectionRoot::new(8);
        root.add_subcollection(b"c".to_vec());
        root.add_subcollection(b"a".to_vec());
        root.add_subcollection(b"b".to_vec());
        let names: Vec<&[u8]> = root.subcollections().collect();
        assert_eq!(names, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let mk = |order: &[&[u8]]| {
            let mut r = CollectionRoot::new(16);
            for n in order {
                r.add_subcollection(n.to_vec());
            }
            r
        };
        let a = mk(&[b"c", b"a", b"b"]);
        let b = mk(&[b"a", b"b", b"c"]);
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn encoding_is_canonical_regardless_of_holder_order() {
        let mk = |holders: Vec<TxId>| {
            let mut r = CollectionRoot::new(16);
            r.set_membership_lock(LockType::Read, holders);
            r
        };
        let a = mk(vec![TxId::from_bytes(vec![3]), TxId::from_bytes(vec![1])]);
        let b = mk(vec![TxId::from_bytes(vec![1]), TxId::from_bytes(vec![3])]);
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn clear_membership_lock_drops_holders() {
        let mut root = CollectionRoot::new(4);
        root.set_membership_lock(LockType::Write, [TxId::from_bytes(vec![7])]);
        root.clear_membership_lock();
        assert_eq!(root.membership_lock(), LockType::None);
        assert!(root.membership_locked_by().is_empty());
    }

    // Golden vector: a fixed root must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let mut root = CollectionRoot::new(1024);
        root.add_subcollection(b"users".to_vec());
        root.set_membership_lock(LockType::Write, [TxId::from_bytes(vec![0xaa, 0xbb])]);
        let got = root.encode();
        let want = [
            0x08, 0x80, 0x08, 0x12, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x18, 0x03, 0x22, 0x02,
            0xaa, 0xbb,
        ];
        assert_eq!(got, want, "collection-root encoding drifted: {got:02x?}");
    }
}
