//! Persistent identity for one logical GlassDB database.

use crate::entropy::fill_random;

/// Number of bytes in a database ID.
pub const DATABASE_ID_BYTES: usize = 16;

/// The persistent ID written into database metadata when the database is
/// first created.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DatabaseId([u8; DATABASE_ID_BYTES]);

impl DatabaseId {
    /// Generates a random 128-bit database ID.
    pub fn new_random() -> Self {
        let mut bytes = [0; DATABASE_ID_BYTES];
        fill_random(&mut bytes);
        Self(bytes)
    }

    /// Builds a database ID from its exact 16-byte representation.
    pub fn from_bytes(bytes: [u8; DATABASE_ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the ID's 16-byte representation.
    pub fn as_bytes(&self) -> &[u8; DATABASE_ID_BYTES] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_bytes_round_trip() {
        let bytes = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let id = DatabaseId::from_bytes(bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }
}
