//! Validated collection names.

use std::fmt;
use std::sync::Arc;

/// Maximum number of raw bytes in one collection name.
pub const MAX_COLLECTION_NAME_BYTES: usize = 255;

/// The raw bytes that name one collection in the binding of its parent.
///
/// GlassDB does not normalize names, so names order by their raw bytes.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionName(Arc<[u8]>);

impl CollectionName {
    /// Builds a name from raw bytes that are within the size limit.
    pub fn new(name: impl AsRef<[u8]>) -> Result<Self, InvalidCollectionName> {
        let name = name.as_ref();
        if name.is_empty() || name.len() > MAX_COLLECTION_NAME_BYTES {
            return Err(InvalidCollectionName);
        }
        Ok(Self(Arc::from(name)))
    }

    /// Returns the raw name bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for CollectionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CollectionName(\"{}\")", self.0.escape_ascii())
    }
}

/// A collection name is empty or longer than [`MAX_COLLECTION_NAME_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCollectionName;

impl fmt::Display for InvalidCollectionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "collection name must contain 1..={MAX_COLLECTION_NAME_BYTES} bytes"
        )
    }
}

impl std::error::Error for InvalidCollectionName {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_must_be_within_the_size_limit() {
        for valid in [vec![0], vec![0xff; MAX_COLLECTION_NAME_BYTES]] {
            assert_eq!(CollectionName::new(&valid).unwrap().as_bytes(), valid);
        }
        for invalid in [Vec::new(), vec![b'x'; MAX_COLLECTION_NAME_BYTES + 1]] {
            assert_eq!(CollectionName::new(invalid), Err(InvalidCollectionName));
        }
    }
}
