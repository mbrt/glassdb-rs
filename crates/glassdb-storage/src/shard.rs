//! The shard object: in-memory view and canonical protobuf encoding (ADR-017).
//!
//! A shard is the coordination unit for a contiguous range of keys (the leaf
//! body of the ADR-031 B-link tree): it is at once the per-key lock table, the
//! MVCC current-writer index, and the key directory. Its body is the
//! compare-and-swap unit, so the encoding is canonical (entries sorted by key,
//! holder sets sorted) and golden-anchored.
//!
//! This module defines an inert data type plus encode/decode and a pure
//! [`Shard::lookup`]. It has no mutation policy and does no I/O; lock
//! transitions, the protocol, and GC are added by later ADRs.

use std::collections::BTreeMap;

use glassdb_data::TxId;
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::lock::LockType;

/// One key's coordination state within a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardEntry {
    /// Raw user key bytes; also the entry's sort key.
    pub key: Vec<u8>,
    /// Lock currently held on the key.
    pub lock_type: LockType,
    /// Transactions holding the lock (more than one only for read locks).
    pub locked_by: Vec<TxId>,
    /// Transaction object holding the committed value (the MVCC pointer), or
    /// `None` if the key has no committed value yet.
    pub current_writer: Option<TxId>,
    /// Tombstone flag.
    pub deleted: bool,
}

impl ShardEntry {
    /// Reports whether the key exists: it has a committed value and is not
    /// tombstoned.
    pub fn exists(&self) -> bool {
        self.current_writer.is_some() && !self.deleted
    }

    /// Reports whether the entry records nothing worth keeping: no lock holder
    /// and no committed writer (not even a tombstone, which always keeps a
    /// `current_writer`). Such an entry names no transaction and is
    /// indistinguishable from an absent one, so a mutation that leaves it this
    /// way may drop it.
    pub fn is_vestigial(&self) -> bool {
        self.locked_by.is_empty() && self.current_writer.is_none()
    }
}

/// A decoded shard: the coordination directory for the keys that map to it.
///
/// Entries are stored keyed by their raw key bytes, so iteration and encoding
/// are in canonical key order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Shard {
    entries: BTreeMap<Vec<u8>, ShardEntry>,
}

impl Shard {
    /// Creates an empty shard.
    pub fn new() -> Self {
        Shard::default()
    }

    /// Builds a shard from entries, keyed by their `key`. If two entries share a
    /// key the later one wins.
    pub fn from_entries<I: IntoIterator<Item = ShardEntry>>(entries: I) -> Self {
        let entries = entries.into_iter().map(|e| (e.key.clone(), e)).collect();
        Shard { entries }
    }

    /// Returns the entry for `key`, or `None` if the shard has no record of it.
    pub fn lookup(&self, key: &[u8]) -> Option<&ShardEntry> {
        self.entries.get(key)
    }

    /// Reports whether `key` exists (has a committed value and is not
    /// tombstoned).
    pub fn exists(&self, key: &[u8]) -> bool {
        self.lookup(key).is_some_and(ShardEntry::exists)
    }

    /// Iterates the entries in canonical (key-sorted) order.
    pub fn entries(&self) -> impl Iterator<Item = &ShardEntry> {
        self.entries.values()
    }

    /// Number of entries in the shard.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reports whether the shard has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Splits the shard at its median key: retains the lower half in `self` and
    /// returns the upper half together with the split key — the first key of the
    /// upper half, which is the inclusive lower bound of the returned shard (and
    /// the exclusive high-key of the retained one). The single home for the
    /// B-link leaf half-split (ADR-031). Requires at least two entries; the
    /// caller must not split a shard that cannot be divided (a single hot key).
    pub fn split_off_median(&mut self) -> (Shard, Vec<u8>) {
        debug_assert!(
            self.entries.len() >= 2,
            "cannot split a shard with fewer than two entries"
        );
        let mid = self.entries.len() / 2;
        let split_key = self
            .entries
            .keys()
            .nth(mid)
            .cloned()
            .expect("median index is in range");
        // `split_off` keeps keys < split_key in `self` and returns keys >=.
        let upper = self.entries.split_off(&split_key);
        (Shard { entries: upper }, split_key)
    }

    /// Encodes the shard to its canonical protobuf body (the CAS unit).
    pub fn encode(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    /// The encoded body length in bytes without materializing the bytes — a
    /// cheap byte-cap check for the split-candidate feed (ADR-031).
    pub fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    /// Decodes a shard from its protobuf body.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::Shard::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling shard", e))?;
        Ok(Shard::from_pb(raw))
    }

    /// Builds the canonical protobuf message for the shard's entries. Shared with
    /// the B-link leaf encoding (ADR-031), where a leaf embeds this as a node
    /// body.
    pub(crate) fn to_pb(&self) -> pb::Shard {
        let entries = self.entries.values().map(entry_to_proto).collect();
        pb::Shard { entries }
    }

    /// Rebuilds a shard from its protobuf message, the inverse of [`to_pb`].
    ///
    /// [`to_pb`]: Self::to_pb
    pub(crate) fn from_pb(raw: pb::Shard) -> Self {
        let entries = raw
            .entries
            .into_iter()
            .map(|e| {
                let entry = entry_from_proto(e);
                (entry.key.clone(), entry)
            })
            .collect();
        Shard { entries }
    }
}

fn entry_to_proto(e: &ShardEntry) -> pb::ShardEntry {
    // Sort the holder set so logically equal entries encode to identical bytes.
    let mut locked_by: Vec<Vec<u8>> = e.locked_by.iter().map(|t| t.as_bytes().to_vec()).collect();
    locked_by.sort();
    pb::ShardEntry {
        key: e.key.clone(),
        lock_type: lock_type_to_proto(e.lock_type) as i32,
        locked_by,
        current_writer: e
            .current_writer
            .as_ref()
            .map(|t| t.as_bytes().to_vec())
            .unwrap_or_default(),
        deleted: e.deleted,
    }
}

fn entry_from_proto(e: pb::ShardEntry) -> ShardEntry {
    let current_writer = (!e.current_writer.is_empty()).then(|| TxId::from_bytes(e.current_writer));
    ShardEntry {
        key: e.key,
        lock_type: lock_type_from_proto(e.lock_type),
        locked_by: e.locked_by.into_iter().map(TxId::from_bytes).collect(),
        current_writer,
        deleted: e.deleted,
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

    fn entry(key: &[u8]) -> ShardEntry {
        ShardEntry {
            key: key.to_vec(),
            lock_type: LockType::None,
            locked_by: Vec::new(),
            current_writer: None,
            deleted: false,
        }
    }

    #[test]
    fn round_trip() {
        let shard = Shard::from_entries([
            ShardEntry {
                key: b"alpha".to_vec(),
                lock_type: LockType::Write,
                locked_by: vec![TxId::from_bytes(vec![1, 2, 3, 4])],
                current_writer: Some(TxId::from_bytes(vec![9, 9])),
                deleted: false,
            },
            ShardEntry {
                key: b"beta".to_vec(),
                lock_type: LockType::Read,
                locked_by: vec![TxId::from_bytes(vec![5]), TxId::from_bytes(vec![6])],
                current_writer: None,
                deleted: false,
            },
            ShardEntry {
                key: b"gamma".to_vec(),
                lock_type: LockType::None,
                locked_by: Vec::new(),
                current_writer: Some(TxId::from_bytes(vec![7])),
                deleted: true,
            },
        ]);

        let decoded = Shard::decode(&shard.encode()).unwrap();
        assert_eq!(decoded, shard);
    }

    #[test]
    fn empty_round_trip() {
        let shard = Shard::new();
        assert!(shard.is_empty());
        let decoded = Shard::decode(&shard.encode()).unwrap();
        assert_eq!(decoded, shard);
        assert!(decoded.is_empty());
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let a = Shard::from_entries([entry(b"c"), entry(b"a"), entry(b"b")]);
        let b = Shard::from_entries([entry(b"a"), entry(b"b"), entry(b"c")]);
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn encoding_is_canonical_regardless_of_holder_order() {
        let mk = |holders: Vec<TxId>| {
            Shard::from_entries([ShardEntry {
                key: b"k".to_vec(),
                lock_type: LockType::Read,
                locked_by: holders,
                current_writer: None,
                deleted: false,
            }])
        };
        let a = mk(vec![TxId::from_bytes(vec![3]), TxId::from_bytes(vec![1])]);
        let b = mk(vec![TxId::from_bytes(vec![1]), TxId::from_bytes(vec![3])]);
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn lookup_and_exists() {
        let shard = Shard::from_entries([
            ShardEntry {
                key: b"live".to_vec(),
                lock_type: LockType::None,
                locked_by: Vec::new(),
                current_writer: Some(TxId::from_bytes(vec![1])),
                deleted: false,
            },
            ShardEntry {
                key: b"tombstone".to_vec(),
                lock_type: LockType::None,
                locked_by: Vec::new(),
                current_writer: Some(TxId::from_bytes(vec![2])),
                deleted: true,
            },
            ShardEntry {
                key: b"locked-only".to_vec(),
                lock_type: LockType::Create,
                locked_by: vec![TxId::from_bytes(vec![3])],
                current_writer: None,
                deleted: false,
            },
        ]);

        assert!(shard.exists(b"live"));
        // Tombstoned and not-yet-committed keys do not exist.
        assert!(!shard.exists(b"tombstone"));
        assert!(!shard.exists(b"locked-only"));
        // A key the shard never saw is absent entirely.
        assert!(shard.lookup(b"missing").is_none());
        assert!(!shard.exists(b"missing"));

        let live = shard.lookup(b"live").unwrap();
        assert_eq!(live.current_writer, Some(TxId::from_bytes(vec![1])));
    }

    #[test]
    fn entries_iterate_sorted() {
        let shard = Shard::from_entries([entry(b"c"), entry(b"a"), entry(b"b")]);
        let keys: Vec<&[u8]> = shard.entries().map(|e| e.key.as_slice()).collect();
        assert_eq!(keys, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn split_off_median_partitions_at_the_split_key() {
        // Four entries split into two of two; the split key is the first key of
        // the upper half and is the exclusive bound between the halves.
        let mut lower = Shard::from_entries([
            entry(b"apple"),
            entry(b"cat"),
            entry(b"mango"),
            entry(b"pear"),
        ]);
        let (upper, split_key) = lower.split_off_median();

        assert_eq!(split_key, b"mango");
        let lower_keys: Vec<&[u8]> = lower.entries().map(|e| e.key.as_slice()).collect();
        assert_eq!(lower_keys, vec![b"apple".as_slice(), b"cat"]);
        let upper_keys: Vec<&[u8]> = upper.entries().map(|e| e.key.as_slice()).collect();
        assert_eq!(upper_keys, vec![b"mango".as_slice(), b"pear"]);
        // Every retained key is strictly below the split key; every moved key is
        // at or above it — the invariant descent relies on.
        assert!(
            lower
                .entries()
                .all(|e| e.key.as_slice() < split_key.as_slice())
        );
        assert!(
            upper
                .entries()
                .all(|e| e.key.as_slice() >= split_key.as_slice())
        );
    }

    #[test]
    fn split_off_median_of_odd_count_keeps_smaller_lower_half() {
        // Three entries split 1/2: mid = 3/2 = 1, so one stays and two move.
        let mut lower = Shard::from_entries([entry(b"a"), entry(b"b"), entry(b"c")]);
        let (upper, split_key) = lower.split_off_median();
        assert_eq!(split_key, b"b");
        assert_eq!(lower.len(), 1);
        assert_eq!(upper.len(), 2);
    }

    // Golden vector: a fixed shard must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let shard = Shard::from_entries([ShardEntry {
            key: b"Hello".to_vec(),
            lock_type: LockType::Write,
            locked_by: vec![TxId::from_bytes(vec![1, 2, 3, 4])],
            current_writer: Some(TxId::from_bytes(vec![0xaa, 0xbb])),
            deleted: false,
        }]);
        let got = shard.encode();
        let want = [
            0x0a, 0x13, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a, 0x04, 0x01,
            0x02, 0x03, 0x04, 0x22, 0x02, 0xaa, 0xbb,
        ];
        assert_eq!(got, want, "shard encoding drifted: {got:02x?}");
    }
}
