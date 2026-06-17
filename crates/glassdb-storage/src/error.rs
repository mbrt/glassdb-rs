//! Storage-layer error type.

use glassdb_backend::BackendError;

/// Errors returned by the storage layer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// The object does not exist.
    #[error("object not found")]
    NotFound,
    /// A conditional operation's precondition failed.
    #[error("precondition failed")]
    Precondition,
    /// The operation's outcome is unknown (in doubt): it may or may not have
    /// been applied.
    #[error("storage outcome unknown (in doubt): {0}")]
    Unavailable(String),
    /// A key was not found in a committed transaction log.
    #[error("key not found in committed transaction")]
    KeyNotFound,
    /// Any other storage error (parsing, invariant violations, etc.).
    #[error("{0}")]
    Other(String),
}

impl From<BackendError> for StorageError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => StorageError::NotFound,
            BackendError::Precondition => StorageError::Precondition,
            BackendError::Unavailable(s) => StorageError::Unavailable(s),
            BackendError::Other(s) => StorageError::Other(s),
        }
    }
}
