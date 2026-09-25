//! Fixed-width ID field decoding shared by the storage codecs.

use crate::error::StorageError;

/// Decodes a required ID field, or reports `error` when the bytes are not a
/// valid ID.
pub(crate) fn decode_id<'a, T: TryFrom<&'a [u8]>>(
    bytes: &'a [u8],
    error: &'static str,
) -> Result<T, StorageError> {
    T::try_from(bytes).map_err(|_| StorageError::other(error))
}

/// Decodes an optional ID field, where empty bytes mean that the ID is absent.
pub(crate) fn decode_optional_id<'a, T: TryFrom<&'a [u8]>>(
    bytes: &'a [u8],
    error: &'static str,
) -> Result<Option<T>, StorageError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    decode_id(bytes, error).map(Some)
}
