//! Transaction-engine error type. Mirrors the Go sentinel errors (`ErrRetry`,
//! `ErrAlreadyFinalized`) and control-flow errors used by the commit algorithm,
//! while wrapping storage/backend errors.

use glassdb_backend::{BackendError, Cause};
use glassdb_data::CollectionAddress;
use glassdb_storage::{Requirement, StorageError};

/// Errors produced by the transaction engine.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransError {
    /// An error from the storage/backend layers.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The transaction should be retried from the beginning (Go `ErrRetry`).
    #[error("retry transaction")]
    Retry,
    /// The identity cannot commit: its transaction record already has a
    /// final status, or a create of its pending record can still land
    /// (ADR-111).
    #[error("transaction cannot commit")]
    AlreadyFinalized,
    /// The transaction was aborted by a higher-priority transaction under the
    /// wound-wait rule (Go `ErrWounded`). It must be retried from the beginning
    /// with a renewed identity that preserves the original priority.
    #[error("transaction was wounded")]
    Wounded,
    /// Internal: resolve a stale reference again at the supplied observation bound.
    #[error("retry validation")]
    ValidateRetry(Requirement),
    /// Internal: locking timed out (suspected deadlock).
    #[error("lock timeout")]
    LockTimeout,
    /// Internal: the single read-write fast path is not applicable.
    #[error("cannot validate transaction with multiple writes")]
    NoSingleWrite,
    /// User data cannot fit in the configured coordination-object limit.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The addressed collection was dropped.
    #[error("stale collection handle")]
    StaleCollection,
    /// A CAS on a collection record can still land after this error
    /// (ADR-106). The owner keeps its owner operation unresolved, so its
    /// identity stays pinned.
    #[error("collection record mutation in doubt: {0}")]
    MutationInDoubt(String),
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

    /// Reports whether a write of the identity that got this error can still
    /// land, so that its owner must keep the identity pinned (ADR-106).
    pub(crate) fn write_may_land(&self) -> bool {
        // Each variant is listed, so a new one needs a decision here.
        match self {
            // A CAS on a collection record that adds a holder.
            TransError::MutationInDoubt(_) => true,
            TransError::Storage(_)
            | TransError::Retry
            | TransError::AlreadyFinalized
            | TransError::Wounded
            | TransError::ValidateRetry(_)
            | TransError::LockTimeout
            | TransError::NoSingleWrite
            | TransError::InvalidInput(_)
            | TransError::StaleCollection
            | TransError::Other { .. } => false,
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
            TransError::InvalidInput(msg) => TransError::InvalidInput(format!("{ctx}: {msg}")),
            TransError::MutationInDoubt(msg) => {
                TransError::MutationInDoubt(format!("{ctx}: {msg}"))
            }
            sentinel => sentinel,
        }
    }

    /// Classifies physical absence while addressing a collection, as
    /// [`StorageError::classify_collection_absence`] does.
    #[must_use]
    pub(crate) fn classify_collection_absence(self, collection: &CollectionAddress) -> Self {
        match self {
            TransError::Storage(error) => {
                TransError::Storage(error.classify_collection_absence(collection))
            }
            other => other,
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
/// [`Reader`](crate::reader::Reader) and the
/// [`KeyResolver`](crate::key_resolver::KeyResolver).
pub(crate) fn trans_to_storage(e: TransError) -> StorageError {
    // Each variant is listed, so a new one needs a decision here.
    match e {
        TransError::Storage(s) => s,
        TransError::StaleCollection => StorageError::StaleCollection,
        // A CAS that can still land stays in doubt (ADR-106).
        TransError::MutationInDoubt(msg) => StorageError::Unavailable(msg),
        e @ (TransError::Retry
        | TransError::AlreadyFinalized
        | TransError::Wounded
        | TransError::ValidateRetry(_)
        | TransError::LockTimeout
        | TransError::NoSingleWrite
        | TransError::InvalidInput(_)
        | TransError::Other { .. }) => StorageError::other(e.to_string()),
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

    // ADR-106: a CAS that can still land stays in doubt on the storage
    // surface of the read paths.
    #[test]
    fn a_mutation_in_doubt_stays_in_doubt_as_a_storage_error() {
        assert!(matches!(
            trans_to_storage(TransError::MutationInDoubt("lock".into())),
            StorageError::Unavailable(msg) if msg == "lock"
        ));
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
