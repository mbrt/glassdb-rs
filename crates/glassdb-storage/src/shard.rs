//! The shard object: in-memory view and canonical protobuf encoding (ADR-017).
//!
//! A shard is the coordination unit for a contiguous range of keys (the leaf
//! body of the ADR-031 B-link tree): it is at once the per-key lock table, the
//! MVCC current-writer index, and the key directory. Its body is the
//! compare-and-swap unit, so the encoding is canonical (entries sorted by key,
//! holder sets sorted) and golden-anchored.
//!
//! This module defines inert data types, their pure lock transitions, and
//! canonical encoding. It contains no conflict policy and performs no I/O.

use std::collections::BTreeMap;
use std::sync::Arc;

use glassdb_data::TxId;
use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;
use crate::lock::{EntryLockState, LockType};

/// A key's committed current value (ADR-051): the writer that produced it plus
/// where the value itself lives.
///
/// `writer` is the optimistic-validation token the commit path compares. It
/// identifies the transaction that produced the version, but it is not
/// universally a pointer to a transaction object: a logless commit publishes
/// [`CurrentState::Inline`] without ever writing one.
///
/// An inline value is authoritative latest-value evidence. Readers return it
/// directly, without consulting the writer's transaction status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum CurrentState {
    /// The key has no committed value yet.
    #[default]
    Absent,
    /// The value lives in the writer's transaction object.
    External { writer: TxId },
    /// The value is authoritative here in the leaf entry.
    Inline { writer: TxId, value: Arc<[u8]> },
    /// The writer deleted the key.
    Tombstone { writer: TxId },
}

impl CurrentState {
    /// The transaction that produced this version, or `None` when the key has
    /// no committed value.
    pub fn writer(&self) -> Option<&TxId> {
        match self {
            CurrentState::Absent => None,
            CurrentState::External { writer }
            | CurrentState::Inline { writer, .. }
            | CurrentState::Tombstone { writer } => Some(writer),
        }
    }

    /// The authoritative inline bytes, or `None` when the value is not inline.
    pub fn inline(&self) -> Option<&Arc<[u8]>> {
        match self {
            CurrentState::Inline { value, .. } => Some(value),
            _ => None,
        }
    }

    /// The inline payload's size, or zero when the value is not inline. The
    /// unit of the per-leaf inline budget.
    pub fn inline_len(&self) -> usize {
        self.inline().map_or(0, |value| value.len())
    }

    /// Reports whether the current state is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, CurrentState::Tombstone { .. })
    }

    /// Reports whether the key currently exists: it has a committed value that
    /// is not a tombstone.
    pub fn exists(&self) -> bool {
        matches!(
            self,
            CurrentState::External { .. } | CurrentState::Inline { .. }
        )
    }
}

/// One key's coordination state within a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardEntry {
    /// Raw user key bytes; also the entry's sort key.
    pub key: Vec<u8>,
    /// The key's committed current value, separate from the lock state above.
    pub current: CurrentState,
    lock: EntryLockState,
}

impl ShardEntry {
    /// Creates an unlocked entry for `key` with no committed value.
    pub fn new(key: impl Into<Vec<u8>>) -> Self {
        ShardEntry {
            key: key.into(),
            current: CurrentState::Absent,
            lock: EntryLockState::default(),
        }
    }

    /// Sets the committed current state while preserving this entry's lock.
    pub fn with_current(mut self, current: CurrentState) -> Self {
        self.current = current;
        self
    }

    /// Returns the lock type currently held on this entry.
    pub fn lock_type(&self) -> LockType {
        self.lock.lock_type()
    }

    /// Returns the transactions holding this entry's lock.
    pub fn lock_holders(&self) -> &[TxId] {
        self.lock.holders()
    }

    /// Reports whether `id` holds this entry's lock.
    pub fn is_locked_by(&self, id: &TxId) -> bool {
        self.lock.contains(id)
    }

    /// Acquires a shared-read hold for `holder`.
    pub fn acquire_read_lock(&mut self, holder: TxId) {
        self.lock.acquire_read(holder);
    }

    /// Replaces the entry lock with one exclusive writer.
    pub fn replace_write_lock(&mut self, holder: TxId) {
        self.replace_lock(EntryLockState::write(holder));
    }

    /// Replaces the entry lock with one exclusive creator.
    pub fn replace_create_lock(&mut self, holder: TxId) {
        self.replace_lock(EntryLockState::create(holder));
    }

    /// Replaces the entry lock with a validated state.
    pub fn replace_lock(&mut self, lock: EntryLockState) {
        self.lock = lock;
    }

    /// Releases `holder` from this entry's lock.
    pub fn release_lock(&mut self, holder: &TxId) -> bool {
        self.lock.release(holder)
    }

    /// Reports whether the key exists: it has a committed value and is not
    /// tombstoned.
    pub fn exists(&self) -> bool {
        self.current.exists()
    }

    /// Reports whether the entry records nothing worth keeping: no lock holder
    /// and no committed value (not even a tombstone, which always names a
    /// writer). Such an entry names no transaction and is indistinguishable
    /// from an absent one, so a mutation that leaves it this way may drop it.
    pub fn is_vestigial(&self) -> bool {
        self.lock_holders().is_empty() && matches!(self.current, CurrentState::Absent)
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
        Shard::from_pb(raw)
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
    pub(crate) fn from_pb(raw: pb::Shard) -> Result<Self, StorageError> {
        let mut entries = BTreeMap::new();
        for e in raw.entries {
            let entry = entry_from_proto(e)?;
            if entries.insert(entry.key.clone(), entry).is_some() {
                return Err(StorageError::other(
                    "shard contains duplicate entries for a key",
                ));
            }
        }
        Ok(Shard { entries })
    }
}

fn entry_to_proto(e: &ShardEntry) -> pb::ShardEntry {
    let (lock_type, locked_by) = e.lock.to_wire();
    pb::ShardEntry {
        key: e.key.clone(),
        lock_type,
        locked_by,
        current: current_to_proto(&e.current),
    }
}

fn entry_from_proto(e: pb::ShardEntry) -> Result<ShardEntry, StorageError> {
    let lock = EntryLockState::from_wire(e.lock_type, e.locked_by)
        .map_err(|_| StorageError::other("shard entry has an invalid lock"))?;
    Ok(ShardEntry {
        key: e.key,
        lock,
        current: current_from_proto(e.current)?,
    })
}

fn current_to_proto(current: &CurrentState) -> Option<pb::CurrentState> {
    use pb::current_state::State;

    let (writer, state) = match current {
        CurrentState::Absent => return None,
        CurrentState::External { writer } => (writer, State::External(true)),
        CurrentState::Inline { writer, value } => (writer, State::Inline(value.to_vec())),
        CurrentState::Tombstone { writer } => (writer, State::Tombstone(true)),
    };
    Some(pb::CurrentState {
        writer: writer.as_bytes().to_vec(),
        state: Some(state),
    })
}

fn current_from_proto(raw: Option<pb::CurrentState>) -> Result<CurrentState, StorageError> {
    use pb::current_state::State;

    let Some(raw) = raw else {
        return Ok(CurrentState::Absent);
    };
    // A current value without a writer or without a state tag is not a state
    // any mutation can produce: reject it rather than guess which half is
    // authoritative.
    if raw.writer.is_empty() {
        return Err(StorageError::other(
            "shard entry current value has no writer",
        ));
    }
    let writer = TxId::from_bytes(raw.writer);
    match raw.state {
        Some(State::External(_)) => Ok(CurrentState::External { writer }),
        Some(State::Inline(value)) => Ok(CurrentState::Inline {
            writer,
            value: Arc::from(value),
        }),
        Some(State::Tombstone(_)) => Ok(CurrentState::Tombstone { writer }),
        None => Err(StorageError::other(
            "shard entry current value has no state tag",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::lock::lock_type_to_proto;

    fn entry(key: &[u8]) -> ShardEntry {
        ShardEntry::new(key)
    }

    fn tx(bytes: &[u8]) -> TxId {
        TxId::from_bytes(bytes.to_vec())
    }

    fn encode_entry(entry: &ShardEntry) -> Vec<u8> {
        Shard::from_entries([entry.clone()]).encode()
    }

    fn with_lock(mut entry: ShardEntry, lock: EntryLockState) -> ShardEntry {
        entry.replace_lock(lock);
        entry
    }

    #[test]
    fn shared_entry_lock_acquisition_is_canonical_and_idempotent() {
        let first = tx(&[1]);
        let second = tx(&[2]);
        let mut entry = ShardEntry::new(b"key");

        entry.acquire_read_lock(second.clone());
        entry.acquire_read_lock(first.clone());
        assert_eq!(entry.lock_type(), LockType::Read);
        assert_eq!(entry.lock_holders(), &[first.clone(), second.clone()]);
        assert!(entry.is_locked_by(&first));

        let encoded = encode_entry(&entry);
        entry.acquire_read_lock(first.clone());
        assert_eq!(encode_entry(&entry), encoded);

        let mut reverse = ShardEntry::new(b"key");
        reverse.acquire_read_lock(first.clone());
        reverse.acquire_read_lock(second.clone());
        assert_eq!(encode_entry(&reverse), encoded);

        let mut replacement = EntryLockState::read(second);
        replacement.acquire_read(first.clone());
        assert_eq!(replacement.lock_type(), LockType::Read);
        assert!(replacement.contains(&first));
        assert!(replacement.release(&first));
        assert!(!replacement.is_unlocked());
        replacement.acquire_read(first.clone());
        reverse.replace_lock(replacement);
        assert_eq!(encode_entry(&reverse), encoded);

        assert!(reverse.release_lock(&first));
        assert_eq!(reverse.lock_type(), LockType::Read);
        assert_eq!(reverse.lock_holders(), &[tx(&[2])]);
        let released_encoded = encode_entry(&reverse);
        assert!(!reverse.release_lock(&first));
        assert_eq!(encode_entry(&reverse), released_encoded);
    }

    #[test]
    fn exclusive_entry_lock_replacement_and_release_are_idempotent() {
        let writer = tx(&[3]);
        let creator = tx(&[4]);
        let unrelated = tx(&[5]);
        let mut entry = ShardEntry::new(b"key");

        entry.replace_write_lock(writer.clone());
        assert_eq!(entry.lock_type(), LockType::Write);
        assert_eq!(entry.lock_holders(), std::slice::from_ref(&writer));
        let write_encoded = encode_entry(&entry);
        entry.replace_write_lock(writer.clone());
        assert_eq!(encode_entry(&entry), write_encoded);
        assert!(entry.release_lock(&writer));
        assert_eq!(entry.lock_type(), LockType::None);
        assert!(entry.lock_holders().is_empty());
        assert!(!entry.release_lock(&writer));

        entry.replace_create_lock(creator.clone());
        assert_eq!(entry.lock_type(), LockType::Create);
        assert_eq!(entry.lock_holders(), std::slice::from_ref(&creator));
        let create_encoded = encode_entry(&entry);
        entry.replace_create_lock(creator.clone());
        assert_eq!(encode_entry(&entry), create_encoded);

        assert!(!entry.release_lock(&unrelated));
        assert_eq!(encode_entry(&entry), create_encoded);
        assert!(entry.release_lock(&creator));
        assert_eq!(entry.lock_type(), LockType::None);
        assert!(entry.lock_holders().is_empty());
        let unlocked_encoded = encode_entry(&entry);
        assert!(!entry.release_lock(&creator));
        assert_eq!(encode_entry(&entry), unlocked_encoded);
    }

    #[test]
    fn round_trip() {
        let mut read_lock = EntryLockState::read(tx(&[5]));
        read_lock.acquire_read(tx(&[6]));
        let shard = Shard::from_entries([
            with_lock(
                ShardEntry::new(b"alpha").with_current(CurrentState::External {
                    writer: tx(&[9, 9]),
                }),
                EntryLockState::write(tx(&[1, 2, 3, 4])),
            ),
            with_lock(ShardEntry::new(b"beta"), read_lock),
            ShardEntry::new(b"gamma").with_current(CurrentState::Tombstone { writer: tx(&[7]) }),
            ShardEntry::new(b"delta").with_current(CurrentState::Inline {
                writer: tx(&[8]),
                value: Arc::from(b"hello".as_slice()),
            }),
        ]);

        let decoded = Shard::decode(&shard.encode()).unwrap();
        assert_eq!(decoded, shard);
    }

    // An empty inline value is a real value, not an absent one: the `state` tag
    // carries the distinction even though the payload has no bytes.
    #[test]
    fn empty_inline_value_is_distinct_from_external() {
        let inline =
            Shard::from_entries([ShardEntry::new(b"k").with_current(CurrentState::Inline {
                writer: tx(&[1]),
                value: Arc::from(b"".as_slice()),
            })]);
        let external = Shard::from_entries([
            ShardEntry::new(b"k").with_current(CurrentState::External { writer: tx(&[1]) })
        ]);

        assert_ne!(inline.encode(), external.encode());
        assert_eq!(Shard::decode(&inline.encode()).unwrap(), inline);
        assert_eq!(Shard::decode(&external.encode()).unwrap(), external);
    }

    // No mutation can publish a current value without a writer or without a
    // state tag, so decoding one is corrupt state rather than a default.
    #[test]
    fn decoding_rejects_incomplete_current_values() {
        let no_state = pb::Shard {
            entries: vec![pb::ShardEntry {
                key: b"k".to_vec(),
                current: Some(pb::CurrentState {
                    writer: vec![1],
                    state: None,
                }),
                ..Default::default()
            }],
        };
        let no_writer = pb::Shard {
            entries: vec![pb::ShardEntry {
                key: b"k".to_vec(),
                current: Some(pb::CurrentState {
                    writer: Vec::new(),
                    state: Some(pb::current_state::State::External(true)),
                }),
                ..Default::default()
            }],
        };

        assert!(Shard::decode(&no_state.encode_to_vec()).is_err());
        assert!(Shard::decode(&no_writer.encode_to_vec()).is_err());
    }

    #[test]
    fn decoding_accepts_and_canonicalizes_the_lock_wire_matrix() {
        use pb::lock::LockType as PbLockType;

        let valid_locks = [
            (
                PbLockType::Unknown as i32,
                Vec::new(),
                LockType::None,
                Vec::new(),
            ),
            (99, Vec::new(), LockType::None, Vec::new()),
            (
                PbLockType::None as i32,
                Vec::new(),
                LockType::None,
                Vec::new(),
            ),
            (
                PbLockType::Read as i32,
                vec![vec![2], vec![1]],
                LockType::Read,
                vec![tx(&[1]), tx(&[2])],
            ),
            (
                PbLockType::Write as i32,
                vec![vec![3]],
                LockType::Write,
                vec![tx(&[3])],
            ),
            (
                PbLockType::Create as i32,
                vec![vec![4]],
                LockType::Create,
                vec![tx(&[4])],
            ),
        ];

        for (lock_type, locked_by, expected_type, expected_holders) in valid_locks {
            let raw = pb::Shard {
                entries: vec![pb::ShardEntry {
                    key: b"k".to_vec(),
                    lock_type,
                    locked_by,
                    current: None,
                }],
            };

            let shard = Shard::decode(&raw.encode_to_vec()).unwrap();
            let entry = shard.lookup(b"k").unwrap();
            assert_eq!(entry.lock_type(), expected_type);
            assert_eq!(entry.lock_holders(), expected_holders);

            let canonical = pb::Shard::decode(shard.encode().as_slice()).unwrap();
            assert_eq!(
                canonical.entries[0].lock_type,
                lock_type_to_proto(expected_type) as i32
            );
            assert_eq!(
                canonical.entries[0].locked_by,
                expected_holders
                    .iter()
                    .map(|holder| holder.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn decoding_rejects_inconsistent_entry_locks() {
        use pb::lock::LockType as PbLockType;

        let invalid_locks = [
            (PbLockType::None as i32, vec![vec![1]]),
            (PbLockType::Unknown as i32, vec![vec![1]]),
            (99, vec![vec![1]]),
            (PbLockType::Read as i32, Vec::new()),
            (PbLockType::Read as i32, vec![vec![1], vec![1]]),
            (PbLockType::Write as i32, Vec::new()),
            (PbLockType::Write as i32, vec![vec![1], vec![2]]),
            (PbLockType::Create as i32, Vec::new()),
            (PbLockType::Create as i32, vec![vec![1], vec![2]]),
        ];

        for (lock_type, locked_by) in invalid_locks {
            let raw = pb::Shard {
                entries: vec![pb::ShardEntry {
                    key: b"k".to_vec(),
                    lock_type,
                    locked_by,
                    current: None,
                }],
            };

            let error = Shard::decode(&raw.encode_to_vec()).unwrap_err();
            assert_eq!(error.to_string(), "shard entry has an invalid lock");
        }
    }

    #[test]
    fn decoding_treats_an_unspecified_empty_lock_as_unlocked() {
        let raw = pb::Shard {
            entries: vec![pb::ShardEntry {
                key: b"k".to_vec(),
                ..Default::default()
            }],
        };

        let shard = Shard::decode(&raw.encode_to_vec()).unwrap();
        assert_eq!(shard.lookup(b"k").unwrap().lock_type(), LockType::None);
    }

    #[test]
    fn decoding_rejects_duplicate_entry_keys() {
        let entry = pb::ShardEntry {
            key: b"duplicate".to_vec(),
            lock_type: pb::lock::LockType::None as i32,
            ..Default::default()
        };
        let raw = pb::Shard {
            entries: vec![entry.clone(), entry],
        };

        let error = Shard::decode(&raw.encode_to_vec()).unwrap_err();
        assert_eq!(
            error.to_string(),
            "shard contains duplicate entries for a key"
        );
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
            let mut entry = ShardEntry::new(b"k");
            for holder in holders {
                entry.acquire_read_lock(holder);
            }
            Shard::from_entries([entry])
        };
        let a = mk(vec![TxId::from_bytes(vec![3]), TxId::from_bytes(vec![1])]);
        let b = mk(vec![TxId::from_bytes(vec![1]), TxId::from_bytes(vec![3])]);
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn lookup_and_exists() {
        let locked_only = with_lock(
            ShardEntry::new(b"locked-only"),
            EntryLockState::create(tx(&[3])),
        );
        let shard = Shard::from_entries([
            ShardEntry::new(b"live").with_current(CurrentState::External { writer: tx(&[1]) }),
            ShardEntry::new(b"live-inline").with_current(CurrentState::Inline {
                writer: tx(&[4]),
                value: Arc::from(b"v".as_slice()),
            }),
            ShardEntry::new(b"tombstone")
                .with_current(CurrentState::Tombstone { writer: tx(&[2]) }),
            locked_only,
        ]);

        assert!(shard.exists(b"live"));
        assert!(shard.exists(b"live-inline"));
        // Tombstoned and not-yet-committed keys do not exist.
        assert!(!shard.exists(b"tombstone"));
        assert!(!shard.exists(b"locked-only"));
        // A key the shard never saw is absent entirely.
        assert!(shard.lookup(b"missing").is_none());
        assert!(!shard.exists(b"missing"));

        let live = shard.lookup(b"live").unwrap();
        assert_eq!(live.current.writer(), Some(&tx(&[1])));
        assert_eq!(live.current.inline(), None);

        let inline = shard.lookup(b"live-inline").unwrap();
        assert_eq!(inline.current.writer(), Some(&tx(&[4])));
        assert_eq!(
            inline.current.inline().map(AsRef::as_ref),
            Some(b"v" as &[u8])
        );
        assert_eq!(inline.current.inline_len(), 1);

        assert!(shard.lookup(b"tombstone").unwrap().current.is_tombstone());
        assert!(
            shard
                .lookup(b"locked-only")
                .unwrap()
                .current
                .writer()
                .is_none()
        );
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
        let entry = ShardEntry::new(b"Hello").with_current(CurrentState::External {
            writer: tx(&[0xaa, 0xbb]),
        });
        let shard =
            Shard::from_entries([with_lock(entry, EntryLockState::write(tx(&[1, 2, 3, 4])))]);
        let got = shard.encode();
        let want = [
            0x0a, 0x17, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a, 0x04, 0x01,
            0x02, 0x03, 0x04, 0x22, 0x06, 0x0a, 0x02, 0xaa, 0xbb, 0x10, 0x01,
        ];
        assert_eq!(got, want, "shard encoding drifted: {got:02x?}");
    }

    // Golden vector for the inline current state (ADR-051).
    #[test]
    fn golden_inline_encoding() {
        let entry = ShardEntry::new(b"Hello").with_current(CurrentState::Inline {
            writer: tx(&[0xaa, 0xbb]),
            value: Arc::from(b"hi".as_slice()),
        });
        let shard =
            Shard::from_entries([with_lock(entry, EntryLockState::write(tx(&[1, 2, 3, 4])))]);
        let got = shard.encode();
        let want = [
            0x0a, 0x19, 0x0a, 0x05, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x10, 0x03, 0x1a, 0x04, 0x01,
            0x02, 0x03, 0x04, 0x22, 0x08, 0x0a, 0x02, 0xaa, 0xbb, 0x1a, 0x02, 0x68, 0x69,
        ];
        assert_eq!(got, want, "inline shard encoding drifted: {got:02x?}");
    }
}
