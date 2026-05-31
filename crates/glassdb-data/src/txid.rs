use std::fmt;

use rand::Rng;

/// A transaction identifier. Ported from the Go `data.TxID` (`[]byte`).
///
/// Freshly generated IDs are 128-bit random values, but a `TxId` can hold an
/// arbitrary byte sequence (e.g. when decoded from a storage tag).
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxId(Vec<u8>);

impl TxId {
    /// Generates a new random 128-bit transaction ID.
    pub fn new_random() -> Self {
        let mut b = vec![0u8; 16];
        rand::rng().fill_bytes(&mut b);
        TxId(b)
    }

    /// Wraps raw bytes as a transaction ID.
    pub fn from_bytes(b: impl Into<Vec<u8>>) -> Self {
        TxId(b.into())
    }

    /// Returns the raw bytes of the ID.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the ID and returns the owned bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Reports whether the ID is empty (the nil transaction).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of bytes in the ID.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Display for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxId({self})")
    }
}

impl From<Vec<u8>> for TxId {
    fn from(b: Vec<u8>) -> Self {
        TxId(b)
    }
}

/// A set of transaction IDs optimized for small sizes (linear scan), matching
/// the Go `data.TxIDSet` (`[]TxID`).
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct TxIdSet(Vec<TxId>);

impl TxIdSet {
    /// Creates an empty set.
    pub fn new() -> Self {
        TxIdSet(Vec::new())
    }

    /// Creates a set from the given IDs, deduplicating repeats.
    pub fn from_ids<I: IntoIterator<Item = TxId>>(ids: I) -> Self {
        let mut s = TxIdSet::new();
        for id in ids {
            s.add(id);
        }
        s
    }

    /// Inserts an ID, returning whether it was newly added.
    pub fn add(&mut self, id: TxId) -> bool {
        if self.contains(&id) {
            return false;
        }
        self.0.push(id);
        true
    }

    /// Inserts multiple IDs, deduplicating repeats.
    pub fn add_multi<I: IntoIterator<Item = TxId>>(&mut self, ids: I) {
        for id in ids {
            self.add(id);
        }
    }

    /// Reports whether the set contains `id`.
    pub fn contains(&self, id: &TxId) -> bool {
        self.0.iter().any(|x| x == id)
    }

    /// Returns the index of `id`, or `None` if absent.
    pub fn index_of(&self, id: &TxId) -> Option<usize> {
        self.0.iter().position(|x| x == id)
    }

    /// Number of IDs in the set.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Reports whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over the IDs in insertion order.
    pub fn iter(&self) -> std::slice::Iter<'_, TxId> {
        self.0.iter()
    }

    /// Returns the IDs as a slice.
    pub fn as_slice(&self) -> &[TxId] {
        &self.0
    }
}

impl<'a> IntoIterator for &'a TxIdSet {
    type Item = &'a TxId;
    type IntoIter = std::slice::Iter<'a, TxId>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl IntoIterator for TxIdSet {
    type Item = TxId;
    type IntoIter = std::vec::IntoIter<TxId>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Set difference `a \ b`.
pub fn set_diff(a: &TxIdSet, b: &TxIdSet) -> TxIdSet {
    let mut res = TxIdSet::new();
    for id in a.iter() {
        if !b.contains(id) {
            res.0.push(id.clone());
        }
    }
    res
}

/// Set intersection `a ∩ b`.
pub fn set_intersect(a: &TxIdSet, b: &TxIdSet) -> TxIdSet {
    let mut res = TxIdSet::new();
    for id in a.iter() {
        if b.contains(id) {
            res.0.push(id.clone());
        }
    }
    res
}

/// Set union `a ∪ b`.
pub fn set_union(a: &TxIdSet, b: &TxIdSet) -> TxIdSet {
    let mut res = TxIdSet::from_ids(a.iter().cloned());
    res.add_multi(b.iter().cloned());
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_is_16_bytes_and_hex() {
        let id = TxId::new_random();
        assert_eq!(id.len(), 16);
        assert_eq!(id.to_string().len(), 32);
    }

    #[test]
    fn set_ops() {
        let a = TxId::from_bytes(vec![1]);
        let b = TxId::from_bytes(vec![2]);
        let c = TxId::from_bytes(vec![3]);
        let mut s = TxIdSet::from_ids([a.clone(), b.clone(), a.clone()]);
        assert_eq!(s.len(), 2);
        assert!(s.contains(&a));
        assert!(!s.add(a.clone()));
        assert!(s.add(c.clone()));

        let s1 = TxIdSet::from_ids([a.clone(), b.clone()]);
        let s2 = TxIdSet::from_ids([b.clone(), c.clone()]);
        assert_eq!(set_diff(&s1, &s2), TxIdSet::from_ids([a.clone()]));
        assert_eq!(set_intersect(&s1, &s2), TxIdSet::from_ids([b.clone()]));
        assert_eq!(set_union(&s1, &s2), TxIdSet::from_ids([a, b, c]));
    }
}
