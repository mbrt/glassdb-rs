//! Stable identity for one collection incarnation.

use crate::entropy::fill_random;

/// Number of bytes in a collection ID.
pub const COLLECTION_ID_BYTES: usize = 16;

/// Maximum number of raw bytes in one logical collection name.
pub const MAX_COLLECTION_NAME_BYTES: usize = 255;

/// The opaque identity of one collection incarnation.
///
/// The all-zero value is reserved for the permanent database root. Randomly
/// generated IDs are therefore always non-zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionId([u8; COLLECTION_ID_BYTES]);

impl CollectionId {
    /// Returns the reserved identity of the permanent database root collection.
    pub const fn root() -> Self {
        Self([0; COLLECTION_ID_BYTES])
    }

    /// Generates a fresh non-root collection identity.
    pub fn new_random() -> Self {
        loop {
            let mut bytes = [0; COLLECTION_ID_BYTES];
            fill_random(&mut bytes);
            if bytes != [0; COLLECTION_ID_BYTES] {
                return Self(bytes);
            }
        }
    }

    /// Parses an exact 16-byte collection identity.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let bytes = <[u8; COLLECTION_ID_BYTES]>::try_from(bytes).ok()?;
        Some(Self(bytes))
    }

    /// Returns the ID's canonical byte representation.
    pub const fn as_bytes(&self) -> &[u8; COLLECTION_ID_BYTES] {
        &self.0
    }

    /// Reports whether this is the reserved database-root identity.
    pub fn is_root(self) -> bool {
        self.0 == [0; COLLECTION_ID_BYTES]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_non_root_and_round_trip() {
        let id = CollectionId::new_random();
        assert!(!id.is_root());
        assert_eq!(CollectionId::from_slice(id.as_bytes()), Some(id));
    }

    #[test]
    fn parsing_requires_the_exact_width() {
        assert_eq!(CollectionId::from_slice(&[0; 15]), None);
        assert_eq!(
            CollectionId::from_slice(&[0; COLLECTION_ID_BYTES]),
            Some(CollectionId::root())
        );
    }
}
