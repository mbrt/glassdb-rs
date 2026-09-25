//! Identity of one structural intent.

use glassdb_concurr::entropy::fill_bytes;

use crate::ID_BYTES;

/// The random identity of one structural intent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructuralIntentId([u8; ID_BYTES]);

impl StructuralIntentId {
    /// Generates a fresh random structural intent ID.
    pub fn new_random() -> Self {
        let mut bytes = [0; ID_BYTES];
        fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Builds a structural intent ID from its exact 16-byte representation.
    pub const fn from_bytes(bytes: [u8; ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Parses an exact 16-byte structural intent ID.
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
        let id = StructuralIntentId::new_random();
        assert_eq!(StructuralIntentId::from_slice(id.as_bytes()), Some(id));
    }

    #[test]
    fn parsing_requires_the_exact_width() {
        let bytes = [7; ID_BYTES];
        assert_eq!(
            StructuralIntentId::from_slice(&bytes),
            Some(StructuralIntentId::from_bytes(bytes))
        );
        for invalid in [&[][..], &bytes[..15], &[7; 17][..]] {
            assert_eq!(StructuralIntentId::from_slice(invalid), None);
        }
    }
}
