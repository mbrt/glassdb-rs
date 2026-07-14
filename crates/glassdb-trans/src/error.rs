//! Transaction-engine error type. Mirrors the Go sentinel errors (`ErrRetry`,
//! `ErrAlreadyFinalized`) and control-flow errors used by the commit algorithm,
//! while wrapping storage/backend errors.

use glassdb_backend::{BackendError, Cause};
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
    /// A create would push its leaf past the hard object-size cap
    /// (`backend_limit − H`, ADR-032). Retryable: the background split will
    /// relieve the leaf, after which the create fits. The db retry loop re-runs
    /// the transaction after a backoff, so an over-full leaf never wedges — it
    /// only delays a create until the split lands.
    #[error("leaf full, split pending")]
    LeafFull,
    /// Internal: the single read-write fast path is not applicable.
    #[error("cannot validate transaction with multiple writes")]
    NoSingleWrite,
    /// Any other transaction error, with an optional underlying cause.
    #[error("{msg}")]
    Other {
        msg: String,
        #[source]
        source: Option<Cause>,
    },
}

impl TransError {
    /// Builds a [`TransError::Other`] from a message, with no underlying cause.
    pub fn other(msg: impl Into<String>) -> Self {
        TransError::Other {
            msg: msg.into(),
            source: None,
        }
    }

    /// Builds a [`TransError::Other`] that wraps an underlying cause, kept in the
    /// [`std::error::Error::source`] chain.
    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        TransError::Other {
            msg: msg.into(),
            source: Some(Cause::new(source)),
        }
    }

    /// Prepends human-readable context while preserving the error's
    /// classification and any underlying cause.
    #[must_use]
    pub fn context(self, ctx: impl std::fmt::Display) -> Self {
        match self {
            TransError::Storage(s) => TransError::Storage(s.context(ctx)),
            TransError::Other { msg, source } => TransError::Other {
                msg: format!("{ctx}: {msg}"),
                source,
            },
            sentinel => sentinel,
        }
    }
}

impl From<BackendError> for TransError {
    fn from(e: BackendError) -> Self {
        TransError::Storage(e.into())
    }
}

/// Converts a transaction-engine error into a storage error, for the read /
/// resolution paths whose public surface is `StorageError`. Shared by the
/// [`Reader`](crate::Reader) and the [`Resolver`](crate::Resolver).
pub(crate) fn trans_to_storage(e: TransError) -> StorageError {
    match e {
        TransError::Storage(s) => s,
        other => StorageError::other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;
    use std::io;

    fn cause() -> io::Error {
        io::Error::other("disk on fire")
    }

    #[test]
    fn context_preserves_storage_in_doubt() {
        let e = TransError::Storage(StorageError::Unavailable("lost ack".into()))
            .context("failed validation");
        match e {
            TransError::Storage(StorageError::Unavailable(msg)) => {
                assert_eq!(msg, "failed validation: lost ack");
            }
            other => panic!("expected in-doubt storage error, got {other:?}"),
        }
    }

    #[test]
    fn context_preserves_other_source() {
        let e = TransError::with_source("dedup work", cause()).context("locking");
        assert_eq!(e.to_string(), "locking: dedup work");
        assert_eq!(e.source().unwrap().to_string(), "disk on fire");
    }

    #[test]
    fn context_leaves_sentinels_untouched() {
        assert!(matches!(TransError::Retry.context("x"), TransError::Retry));
        assert!(matches!(
            TransError::AlreadyFinalized.context("x"),
            TransError::AlreadyFinalized
        ));
    }
}
