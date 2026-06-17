//! Transaction-engine error type. Mirrors the Go sentinel errors (`ErrRetry`,
//! `ErrAlreadyFinalized`) and control-flow errors used by the commit algorithm,
//! while wrapping storage/backend errors.

use glassdb_backend::BackendError;
use glassdb_storage::StorageError;

/// Errors produced by the transaction engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransError {
    /// An error from the storage/backend layers.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The transaction should be retried from the beginning (Go `ErrRetry`).
    #[error("retry transaction")]
    Retry,
    /// The remote transaction log was already committed or aborted.
    #[error("transaction was already finalized")]
    AlreadyFinalized,
    /// The transaction was aborted by a higher-priority transaction under the
    /// wound-wait rule (Go `ErrWounded`). It must be retried from the beginning
    /// with a fresh attempt that preserves the original priority.
    #[error("transaction was wounded")]
    Wounded,
    /// Internal: re-run validation without retrying the whole transaction.
    #[error("retry validation")]
    ValidateRetry,
    /// Internal: locking timed out (suspected deadlock).
    #[error("lock timeout")]
    LockTimeout,
    /// Internal: the single read-write fast path is not applicable.
    #[error("cannot validate transaction with multiple writes")]
    NoSingleWrite,
    /// Any other transaction error.
    #[error("{0}")]
    Other(String),
}

impl From<BackendError> for TransError {
    fn from(e: BackendError) -> Self {
        TransError::Storage(e.into())
    }
}
