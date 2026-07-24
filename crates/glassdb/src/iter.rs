//! Iterators over collection keys and sub-collections. The listing is resolved
//! up front, so these iterate an in-memory snapshot.

use crate::Collection;
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

/// One immediate child returned by a collection listing.
#[derive(Clone)]
pub struct CollectionEntry {
    /// The raw child name.
    pub name: Vec<u8>,
    /// A handle bound to the listed incarnation.
    pub collection: Collection,
}

impl CollectionEntry {
    pub(crate) fn new(name: Vec<u8>, collection: Collection) -> Self {
        Self { name, collection }
    }
}

/// Iterates over immediate child bindings in name order.
pub struct CollectionsIter {
    items: std::vec::IntoIter<CollectionEntry>,
}

impl CollectionsIter {
    pub(crate) fn new(items: Vec<CollectionEntry>) -> Self {
        CollectionsIter {
            items: items.into_iter(),
        }
    }
}

impl Iterator for CollectionsIter {
    type Item = Result<CollectionEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(Ok)
    }
}
