//! Storage-layer error type.

use glassdb_backend::{BackendError, Cause};

/// Errors returned by the storage layer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum StorageError {
    /// The object does not exist.
    #[error("object not found")]
    NotFound,
    /// A conditional operation's precondition failed.
    #[error("precondition failed")]
    Precondition,
    /// A paginated listing cursor was rejected and that prefix must restart.
    #[error("invalid listing cursor")]
    InvalidCursor,
    /// The operation's outcome is unknown (in doubt): it may or may not have
    /// been applied.
    #[error("storage outcome unknown (in doubt): {0}")]
    Unavailable(String),
    /// A key was not found in a committed transaction log.
    #[error("key not found in committed transaction")]
    KeyNotFound,
    /// Any other storage error (parsing, invariant violations, etc.), with an
    /// optional underlying cause.
    #[error("{msg}")]
    Other {
        msg: String,
        #[source]
        source: Option<Cause>,
    },
}

impl StorageError {
    /// Builds a [`StorageError::Other`] from a message, with no underlying cause.
    pub fn other(msg: impl Into<String>) -> Self {
        StorageError::Other {
            msg: msg.into(),
            source: None,
        }
    }

    /// Builds a [`StorageError::Other`] that wraps an underlying cause, kept in
    /// the [`std::error::Error::source`] chain.
    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        StorageError::Other {
            msg: msg.into(),
            source: Some(Cause::new(source)),
        }
    }

    /// Prepends human-readable context while preserving the error's
    /// classification and any underlying cause.
    #[must_use]
    pub fn context(self, ctx: impl std::fmt::Display) -> Self {
        match self {
            StorageError::Unavailable(s) => StorageError::Unavailable(format!("{ctx}: {s}")),
            StorageError::Other { msg, source } => StorageError::Other {
                msg: format!("{ctx}: {msg}"),
                source,
            },
            classified => classified,
        }
    }
}

impl From<BackendError> for StorageError {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::NotFound => StorageError::NotFound,
            BackendError::Precondition => StorageError::Precondition,
            BackendError::InvalidCursor => StorageError::InvalidCursor,
            BackendError::Unavailable(s) => StorageError::Unavailable(s),
            BackendError::Other { msg, source } => StorageError::Other { msg, source },
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
    fn with_source_keeps_cause_in_chain() {
        let e = StorageError::with_source("reading body", cause());
        assert_eq!(e.to_string(), "reading body");
        assert_eq!(e.source().unwrap().to_string(), "disk on fire");
    }

    #[test]
    fn context_prefixes_message_and_preserves_source() {
        let e = StorageError::with_source("reading body", cause()).context("loading metadata");
        assert_eq!(e.to_string(), "loading metadata: reading body");
        assert_eq!(e.source().unwrap().to_string(), "disk on fire");
    }

    #[test]
    fn context_preserves_in_doubt_classification() {
        let e = StorageError::Unavailable("lost ack".into()).context("committing");
        assert!(matches!(e, StorageError::Unavailable(_)));
        assert_eq!(
            e.to_string(),
            "storage outcome unknown (in doubt): committing: lost ack"
        );
    }

    #[test]
    fn context_leaves_classified_variants_untouched() {
        assert!(matches!(
            StorageError::NotFound.context("x"),
            StorageError::NotFound
        ));
        assert!(matches!(
            StorageError::Precondition.context("x"),
            StorageError::Precondition
        ));
        assert!(matches!(
            StorageError::InvalidCursor.context("x"),
            StorageError::InvalidCursor
        ));
    }

    #[test]
    fn from_backend_moves_cause_through() {
        let backend = BackendError::with_source("gcs request", cause());
        let storage: StorageError = backend.into();
        assert!(matches!(storage, StorageError::Other { .. }));
        assert_eq!(storage.source().unwrap().to_string(), "disk on fire");
    }
}
