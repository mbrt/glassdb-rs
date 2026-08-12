//! Iterators over collection keys and sub-collections. The listing is resolved
//! up front, so these iterate an in-memory snapshot.

use std::iter::FusedIterator;

use crate::Collection;
use crate::error::Error;

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

struct FallibleIter<I> {
    items: I,
}

impl<I> FallibleIter<I> {
    fn new(items: I) -> Self {
        Self { items }
    }
}

impl<I: Iterator> Iterator for FallibleIter<I> {
    type Item = Result<I::Item, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.items.size_hint()
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for FallibleIter<I> {}
impl<I: FusedIterator> FusedIterator for FallibleIter<I> {}

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

/// Iterates over the keys in a collection.
///
/// In v2 keys are resolved from the collection's shard objects and decoded by
/// the caller, so this iterator simply yields the pre-decoded, sorted raw keys.
pub struct KeysIter(FallibleIter<KeyIter>);

impl KeysIter {
    pub(crate) fn from_plain(items: KeyIter) -> Self {
        Self(FallibleIter::new(items))
    }
}

impl Iterator for KeysIter {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
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

/// Iterates over immediate child bindings in name order.
pub struct CollectionsIter(FallibleIter<CollectionIter>);

impl CollectionsIter {
    pub(crate) fn from_plain(items: CollectionIter) -> Self {
        Self(FallibleIter::new(items))
    }
}

impl Iterator for CollectionsIter {
    type Item = Result<CollectionEntry, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
