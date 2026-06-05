//! Storage-layer error type.

use glassdb_backend::BackendError;

/// Errors returned by the storage layer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// An error from the underlying backend.
    #[error(transparent)]
    Backend(#[from] BackendError),
    /// A key was not found in a committed transaction log.
    #[error("key not found in committed transaction")]
    KeyNotFound,
    /// Any other storage error (parsing, invariant violations, etc.).
    #[error("{0}")]
    Other(String),
}

impl StorageError {
    /// Reports whether the underlying cause is a not-found error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, StorageError::Backend(b) if b.is_not_found())
    }

    /// Reports whether the underlying cause is a precondition-failed error.
    pub fn is_precondition(&self) -> bool {
        matches!(self, StorageError::Backend(b) if b.is_precondition())
    }

    /// Reports whether the underlying cause is an in-doubt (unknown-outcome)
    /// error: the operation may or may not have been applied.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, StorageError::Backend(b) if b.is_unavailable())
    }
}
