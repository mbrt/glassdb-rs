//! Iterators over collection keys and sub-collections. The listing is resolved
//! up front, so these iterate an in-memory snapshot.

use std::iter::FusedIterator;

use crate::Collection;

struct MaterializedIter<T> {
    items: std::vec::IntoIter<T>,
}

impl<T> MaterializedIter<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            items: items.into_iter(),
        }
    }
}

impl<T> Iterator for MaterializedIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.items.size_hint()
    }
}

impl<T> ExactSizeIterator for MaterializedIter<T> {}
impl<T> FusedIterator for MaterializedIter<T> {}

/// Iterates over materialized collection keys without per-item failure.
///
/// All I/O, decoding, and serializable validation complete before this owned
/// iterator is returned. It yields sorted raw keys and performs no I/O itself.
pub struct KeyIter(MaterializedIter<Vec<u8>>);

impl KeyIter {
    pub(crate) fn new(items: Vec<Vec<u8>>) -> Self {
        Self(MaterializedIter::new(items))
    }
}

impl Iterator for KeyIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl ExactSizeIterator for KeyIter {}
impl FusedIterator for KeyIter {}

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

/// Iterates over materialized child bindings without per-item failure.
///
/// The child directory is materialized before this owned iterator is returned,
/// so iteration performs no I/O and cannot fail. Children are yielded in
/// raw-name order, and every handle remains bound to the listed incarnation.
pub struct CollectionIter(MaterializedIter<CollectionEntry>);

impl CollectionIter {
    pub(crate) fn new(items: Vec<CollectionEntry>) -> Self {
        Self(MaterializedIter::new(items))
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
