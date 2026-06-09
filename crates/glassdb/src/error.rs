//! The public error type for the GlassDB API.

use glassdb_backend::BackendError;
use glassdb_storage::StorageError;
use glassdb_trans::TransError;

/// Errors returned by the GlassDB public API.
///
/// Cancellation is not modeled as an error: a transaction that was cancelled by
/// dropping its future (`tokio::time::timeout`, `select!`, or
/// `JoinHandle::abort`) simply returns nothing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The requested object does not exist.
    #[error("object not found")]
    NotFound,
    /// The transaction was explicitly aborted by the user.
    #[error("aborted transaction")]
    Aborted,
    /// A conditional operation's precondition failed.
    #[error("precondition failed")]
    Precondition,
    /// The transaction was already committed or aborted remotely.
    #[error("transaction was already finalized")]
    AlreadyFinalized,
    /// The transaction's outcome is unknown (in doubt): a storage operation
    /// could not be confirmed, so it may or may not have been applied. The
    /// engine deliberately does *not* retry such a transaction transparently,
    /// because a retry could double-apply a write that actually landed. The
    /// caller decides whether to retry (with its own idempotency) or accept the
    /// uncertainty.
    #[error("transaction outcome unknown (in doubt): {0}")]
    Unavailable(String),
    /// The database is shutting down and is no longer accepting new
    /// transactions. Existing in-flight transactions are allowed to complete.
    #[error("database is shutting down")]
    ShuttingDown,
    /// Any other error.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Reports whether this is a not-found error (mirrors `backend.ErrNotFound`).
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound)
    }

    /// Reports whether this is a precondition-failed error.
    pub fn is_precondition(&self) -> bool {
        matches!(self, Error::Precondition)
    }

    /// Reports whether the transaction was aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self, Error::Aborted)
    }

    /// Reports whether the transaction's outcome is unknown (in doubt). Such a
    /// transaction may or may not have committed; the engine does not retry it
    /// transparently, leaving the decision to the caller.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Error::Unavailable(_))
    }
}

impl From<BackendError> for Error {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => Error::NotFound,
            BackendError::Precondition => Error::Precondition,
            BackendError::Unavailable(s) => Error::Unavailable(s),
            BackendError::Other(s) => Error::Other(s),
        }
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Backend(b) => b.into(),
            StorageError::KeyNotFound => Error::NotFound,
            StorageError::Other(s) => Error::Other(s),
        }
    }
}

impl From<TransError> for Error {
    fn from(e: TransError) -> Self {
        match e {
            TransError::Storage(s) => s.into(),
            TransError::AlreadyFinalized => Error::AlreadyFinalized,
            TransError::Retry => Error::Other("retry transaction".into()),
            other => Error::Other(other.to_string()),
        }
    }
}
