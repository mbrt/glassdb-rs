//! The public error type for the GlassDB API.

use glassdb_backend::{BackendError, Cause};
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
    /// An unexpected internal failure or invariant violation, with an optional
    /// underlying cause kept in the [`std::error::Error::source`] chain.
    #[error("{msg}")]
    Internal {
        msg: String,
        #[source]
        source: Option<Cause>,
    },
}

impl Error {
    /// Builds an [`Error::Internal`] from a message, with no underlying cause.
    pub fn internal(msg: impl Into<String>) -> Self {
        Error::Internal {
            msg: msg.into(),
            source: None,
        }
    }

    /// Builds an [`Error::Internal`] that wraps an underlying cause, kept in the
    /// [`std::error::Error::source`] chain.
    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::Internal {
            msg: msg.into(),
            source: Some(Cause::new(source)),
        }
    }
}

impl From<BackendError> for Error {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => Error::NotFound,
            BackendError::Precondition => Error::Precondition,
            BackendError::Unavailable(s) => Error::InDoubt(s),
            BackendError::Other { msg, source } => Error::Internal { msg, source },
        }
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound | StorageError::KeyNotFound => Error::NotFound,
            StorageError::Precondition => Error::Precondition,
            StorageError::Unavailable(s) => Error::InDoubt(s),
            StorageError::Other { msg, source } => Error::Internal { msg, source },
        }
    }
}

impl From<TransError> for Error {
    fn from(e: TransError) -> Self {
        match e {
            TransError::Storage(s) => s.into(),
            TransError::AlreadyFinalized => Error::AlreadyFinalized,
            TransError::Other { msg, source } => Error::Internal { msg, source },
            TransError::Retry
            | TransError::Wounded
            | TransError::ValidateRetry
            | TransError::LockTimeout
            | TransError::NoSingleWrite => {
                Error::internal(format!("transaction control-flow error escaped: {e}"))
            }
        }
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
    fn backend_cause_survives_the_full_layer_chain() {
        // A backend foreign error wrapped with a cause, then carried up through
        // storage and transaction layers, still exposes the original cause via
        // `source()` on the public error.
        let backend = BackendError::with_source("gcs request", cause());
        let trans = TransError::from(backend).context("locking collections");
        let err = Error::from(trans);

        assert!(matches!(err, Error::Internal { .. }));
        assert_eq!(err.to_string(), "locking collections: gcs request");
        assert_eq!(err.source().unwrap().to_string(), "disk on fire");
    }

    #[test]
    fn in_doubt_classification_is_preserved_not_downgraded() {
        // An in-doubt outcome must surface as `InDoubt`, never collapsed into a
        // generic `Internal`, even after being wrapped with context.
        let trans = TransError::Storage(StorageError::Unavailable("lost ack".into()))
            .context("committing writes");
        let err = Error::from(trans);

        match err {
            Error::InDoubt(msg) => assert_eq!(msg, "committing writes: lost ack"),
            other => panic!("expected in-doubt, got {other:?}"),
        }
    }

    #[test]
    fn with_source_keeps_cause_on_internal() {
        let err = Error::with_source("reading from storage", cause());
        assert_eq!(err.to_string(), "reading from storage");
        assert_eq!(err.source().unwrap().to_string(), "disk on fire");
    }
}
