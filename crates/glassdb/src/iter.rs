//! Iterators over collection keys and sub-collections. The listing is resolved
//! up front, so these iterate an in-memory snapshot.

use crate::error::Error;

/// Iterates over the keys in a collection.
///
/// In v2 keys are resolved from the collection's shard objects and decoded by
/// the caller, so this iterator simply yields the pre-decoded, sorted raw keys.
pub struct KeysIter {
    items: std::vec::IntoIter<Vec<u8>>,
}

impl KeysIter {
    pub(crate) fn new(items: Vec<Vec<u8>>) -> Self {
        KeysIter {
            items: items.into_iter(),
        }
    }
}

impl Iterator for KeysIter {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(Ok)
    }
}

/// Iterates over the sub-collection names within a collection, in name order.
pub struct CollectionsIter {
    items: std::vec::IntoIter<Vec<u8>>,
}

impl CollectionsIter {
    pub(crate) fn new(items: Vec<Vec<u8>>) -> Self {
        CollectionsIter {
            items: items.into_iter(),
        }
    }
}

impl Iterator for CollectionsIter {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(Ok)
    }
}
