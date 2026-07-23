//! Persistent identity for one logical GlassDB database.

use crate::entropy::fill_random;

/// Number of bytes in a database UUID.
pub const DATABASE_UUID_BYTES: usize = 16;

/// The persistent UUID written into database metadata when the database is
/// first created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseUuid([u8; DATABASE_UUID_BYTES]);

impl DatabaseUuid {
    /// Generates a random RFC-compatible version-4 UUID.
    pub fn new_random() -> Self {
        let mut bytes = [0; DATABASE_UUID_BYTES];
        fill_random(&mut bytes);
        // Keep the identifier interoperable with UUID tooling while sourcing
        // entropy through GlassDB's deterministic simulation seam.
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Self(bytes)
    }

    /// Parses an exact RFC-compatible version-4 UUID byte sequence.
    pub fn from_bytes(bytes: [u8; DATABASE_UUID_BYTES]) -> Option<Self> {
        let is_v4 = bytes[6] & 0xf0 == 0x40;
        let is_rfc_variant = bytes[8] & 0xc0 == 0x80;
        (is_v4 && is_rfc_variant).then_some(Self(bytes))
    }

    /// Returns the UUID's canonical 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; DATABASE_UUID_BYTES] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_uuid_has_v4_shape() {
        let uuid = DatabaseUuid::new_random();
        assert_eq!(uuid.as_bytes()[6] & 0xf0, 0x40);
        assert_eq!(uuid.as_bytes()[8] & 0xc0, 0x80);
        assert_eq!(DatabaseUuid::from_bytes(*uuid.as_bytes()), Some(uuid));
    }

    #[test]
    fn rejects_wrong_version_or_variant() {
        let mut bytes = *DatabaseUuid::new_random().as_bytes();
        bytes[6] = (bytes[6] & 0x0f) | 0x30;
        assert_eq!(DatabaseUuid::from_bytes(bytes), None);

        let mut bytes = *DatabaseUuid::new_random().as_bytes();
        bytes[8] &= 0x3f;
        assert_eq!(DatabaseUuid::from_bytes(bytes), None);
    }
}
