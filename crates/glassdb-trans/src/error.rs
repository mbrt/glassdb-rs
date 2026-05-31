//! Transaction-engine error type. Mirrors the Go sentinel errors (`ErrRetry`,
//! `ErrAlreadyFinalized`) and control-flow errors used by the commit algorithm,
//! while wrapping storage/backend errors.

use glassdb_backend::BackendError;
use glassdb_concurr::Cancelled;
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
    /// The context was cancelled.
    #[error("context canceled")]
    Cancelled,
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
        TransError::Storage(StorageError::Backend(e))
    }
}

impl From<Cancelled> for TransError {
    fn from(_: Cancelled) -> Self {
        TransError::Cancelled
    }
}

impl TransError {
    /// Reports whether this is the retry sentinel.
    pub fn is_retry(&self) -> bool {
        matches!(self, TransError::Retry)
    }

    /// Reports whether the underlying cause is a not-found error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, TransError::Storage(s) if s.is_not_found())
    }

    /// Reports whether the underlying cause is a precondition-failed error.
    pub fn is_precondition(&self) -> bool {
        matches!(self, TransError::Storage(s) if s.is_precondition())
    }

    /// Reports whether the context was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, TransError::Cancelled)
            || matches!(
                self,
                TransError::Storage(StorageError::Backend(BackendError::Cancelled))
            )
    }
}
