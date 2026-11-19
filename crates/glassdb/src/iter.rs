//! Iterators over sub-collections. The listing is resolved up front, so they
//! iterate an in-memory snapshot.

use std::iter::FusedIterator;

use crate::Collection;

/// One immediate child returned by a collection listing.
#[derive(Clone)]
pub struct CollectionEntry {
    /// The raw child name.
    pub name: Vec<u8>,
    /// A handle bound to the listed collection ID.
    pub collection: Collection,
}

impl CollectionEntry {
    pub(crate) fn new(name: Vec<u8>, collection: Collection) -> Self {
        Self { name, collection }
    }
}

/// Iterates over materialized child bindings without per-item failure.
///
/// The child directory is materialized before this owned iterator is returned,
/// so iteration performs no I/O and cannot fail. Children are yielded in
/// raw-name order, and every handle remains bound to the listed collection ID.
pub struct CollectionIter(std::vec::IntoIter<CollectionEntry>);

impl CollectionIter {
    pub(crate) fn new(items: Vec<CollectionEntry>) -> Self {
        Self(items.into_iter())
    }
}

impl Iterator for CollectionIter {
    type Item = CollectionEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for CollectionIter {}
impl FusedIterator for CollectionIter {}
