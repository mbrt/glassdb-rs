//! The public error type for the GlassDB API.

use glassdb_backend::BackendError;
use glassdb_storage::StorageError;
use glassdb_trans::TransError;

/// Errors returned by the GlassDB public API.
///
/// Cancellation is not modeled as an error: a transaction that was cancelled by
/// dropping its future (`tokio::time::timeout`, `select!`, or
/// `JoinHandle::abort`) simply returns nothing.
#[non_exhaustive]
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
    InDoubt(String),
    /// The database is shutting down and is no longer accepting new
    /// transactions. Existing in-flight transactions are allowed to complete.
    #[error("database is shutting down")]
    ShuttingDown,
    /// Invalid user input.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// An unexpected internal failure or invariant violation.
    #[error("{0}")]
    Internal(String),
}

impl From<BackendError> for Error {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => Error::NotFound,
            BackendError::Precondition => Error::Precondition,
            BackendError::Unavailable(s) => Error::InDoubt(s),
            BackendError::Other(s) => Error::Internal(s),
        }
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound | StorageError::KeyNotFound => Error::NotFound,
            StorageError::Precondition => Error::Precondition,
            StorageError::Unavailable(s) => Error::InDoubt(s),
            StorageError::Other(s) => Error::Internal(s),
        }
    }
}

impl From<TransError> for Error {
    fn from(e: TransError) -> Self {
        match e {
            TransError::Storage(s) => s.into(),
            TransError::AlreadyFinalized => Error::AlreadyFinalized,
            TransError::Other(s) => Error::Internal(s),
            TransError::Retry
            | TransError::Wounded
            | TransError::ValidateRetry
            | TransError::LockTimeout
            | TransError::NoSingleWrite => {
                Error::Internal(format!("transaction control-flow error escaped: {e}"))
            }
        }
    }
}
