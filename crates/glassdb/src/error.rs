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
    /// A read (or other idempotent operation) could not complete because
    /// storage was unavailable, even after the engine exhausted its in-place
    /// retries. Unlike [`Error::InDoubt`], no mutation is in question — a read
    /// has no side effects — so the operation is always safe to retry.
    #[error("storage unavailable: {0}")]
    Unavailable(String),
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

    /// Maps a storage error produced by a side-effect-free (idempotent) read
    /// into the public error.
    ///
    /// This is the single place the read paths convert their errors, so the
    /// read-vs-commit distinction lives here rather than being duplicated at
    /// every call site. The only difference from the blanket [`From`] is the
    /// `Unavailable` arm: a read puts no mutation in question, so a sustained
    /// outage is the matchable, retry-safe [`Error::Unavailable`] instead of
    /// [`Error::InDoubt`].
    ///
    /// It must be used *only* for genuine reads. Operations that flow through
    /// the commit path — including reads that confirm a pending write, such as
    /// re-reading a transaction's commit status — must keep the conservative
    /// `Unavailable -> InDoubt` mapping of [`From`], because there a failed read
    /// inherits the uncertainty of the mutation it was confirming.
    pub(crate) fn from_read(e: StorageError) -> Self {
        match e {
            StorageError::Unavailable(s) => Error::Unavailable(s),
            other => other.into(),
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
            TransError::InvalidInput(msg) => Error::InvalidInput(msg),
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

    #[test]
    fn unavailable_displays_its_message() {
        // The read path surfaces a sustained outage as `Unavailable`, distinct
        // from the in-doubt mutation case, carrying a human-readable breadcrumb.
        let err = Error::Unavailable("reading k: gcs status 503".into());
        assert_eq!(
            err.to_string(),
            "storage unavailable: reading k: gcs status 503"
        );
        assert!(err.source().is_none());
    }
}
