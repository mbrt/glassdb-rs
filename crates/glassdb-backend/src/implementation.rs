//! Support for implementing and testing [`Backend`](crate::Backend).
//!
//! Applications using an existing backend normally do not need this module.
//! The conformance assertions check the shared object-storage contract. Cursor
//! helpers bind provider continuation tokens to their listing prefixes.

mod conformance;

pub use conformance::{assert_backend_conformance, assert_list_conformance};

use crate::{BackendError, ListCursor};

/// Binds a nonempty provider continuation token to its listing prefix.
pub fn bind_list_cursor(prefix: &str, provider_token: &str) -> Result<ListCursor, BackendError> {
    validate_list_prefix(prefix)?;
    if provider_token.is_empty() {
        return Err(BackendError::other(
            "list provider returned an empty continuation token",
        ));
    }
    Ok(ListCursor {
        prefix: prefix.into(),
        provider_token: provider_token.into(),
    })
}

/// Validates a listing prefix and cursor and returns the provider token.
pub fn list_provider_token<'a>(
    prefix: &str,
    cursor: Option<&'a ListCursor>,
) -> Result<Option<&'a str>, BackendError> {
    validate_list_prefix(prefix)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.prefix.as_ref() != prefix {
        return Err(BackendError::InvalidCursor);
    }
    Ok(Some(cursor.provider_token.as_ref()))
}

fn validate_list_prefix(prefix: &str) -> Result<(), BackendError> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Ok(())
    } else {
        Err(BackendError::other(format!(
            "list prefix must be empty or end in '/': {prefix:?}"
        )))
    }
}
