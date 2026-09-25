//! Identity of one node other than the tree root.

use glassdb_concurr::entropy::fill_bytes;

use crate::ID_BYTES;

/// The random identity of one node other than the tree root.
///
/// Its object-path component uses the order-preserving base64 encoding, so
/// node IDs sort in the same order as their object paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId([u8; ID_BYTES]);

impl NodeId {
    /// Generates a fresh random node ID.
    ///
    /// Random IDs give object-store partitions a high-entropy path prefix. The
    /// entropy source is simulation-aware.
    pub fn new_random() -> Self {
        let mut bytes = [0; ID_BYTES];
        fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Builds a node ID from its exact 16-byte representation.
    pub const fn from_bytes(bytes: [u8; ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Parses an exact 16-byte node ID.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }

    /// Returns the ID's 16-byte representation.
    pub const fn as_bytes(&self) -> &[u8; ID_BYTES] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_round_trip() {
        let id = NodeId::new_random();
        assert_eq!(NodeId::from_slice(id.as_bytes()), Some(id));
    }

    #[test]
    fn parsing_requires_the_exact_width() {
        let bytes = [7; ID_BYTES];
        assert_eq!(NodeId::from_slice(&bytes), Some(NodeId::from_bytes(bytes)));
        // IDs of the older string format have 22 bytes.
        for invalid in [
            &[][..],
            &bytes[..15],
            &[7; 17][..],
            b"0000000000000000000000",
        ] {
            assert_eq!(NodeId::from_slice(invalid), None);
        }
    }
}
