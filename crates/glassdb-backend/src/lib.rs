//! Object-storage backend abstraction (ADR-016, ADR-023).
//!
//! A [`Backend`] is a small, content-CAS-only contract over an object store:
//! reads (plain and version-conditional), writes (unconditional and
//! compare-and-swap), delete, and list. Coordination state lives entirely in
//! object **content**; there are no metadata tags. Every object carries an
//! opaque CAS [`Version`] (its ETag/generation), which is the only token used
//! for conditional reads and writes.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;

pub mod memory;
pub mod middleware;
mod stats;

pub use stats::{BackendStats, StatsBackend};

/// A type-erased, cheaply cloneable underlying cause.
#[derive(Clone)]
pub struct Cause(Arc<dyn std::error::Error + Send + Sync + 'static>);

impl Cause {
    /// Wraps an error as a cause.
    pub fn new(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Cause(Arc::new(source))
    }
}

impl std::fmt::Debug for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for Cause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

/// Errors returned by backend operations.
///
/// Cancellation is not modeled as an error: backend futures are cancelled by
/// being dropped (via `tokio::time::timeout`, `select!`, or
/// `JoinHandle::abort`), and a dropped future simply returns nothing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BackendError {
    /// The object does not exist.
    #[error("object not found")]
    NotFound,
    /// A conditional operation's precondition failed (version mismatch). For a
    /// conditional write this is a CAS miss; for [`Backend::read_if_modified`]
    /// it means "not modified" — the caller's cached copy is still current.
    #[error("precondition failed")]
    Precondition,
    /// A listing cursor is invalid for the requested prefix or was rejected by
    /// the provider. Callers should restart that prefix from the beginning.
    #[error("invalid listing cursor")]
    InvalidCursor,
    /// The operation's outcome is unknown: the request may or may not have been
    /// applied. Returned when a call cannot be completed with a definitive
    /// answer — e.g. a conditional write whose acknowledgement was lost and
    /// whose retry then observed a precondition failure (so it cannot be told
    /// apart from a genuine conflict), or a sustained outage that exhausts the
    /// retry budget. Because the outcome is in doubt, a non-idempotent operation
    /// must *not* be blindly retried; the caller decides how to proceed.
    #[error("storage outcome unknown (in doubt): {0}")]
    Unavailable(String),
    /// Any other backend error, with an optional underlying cause.
    #[error("{msg}")]
    Other {
        msg: String,
        #[source]
        source: Option<Cause>,
    },
}

impl BackendError {
    /// Builds an [`BackendError::Other`] from a message, with no underlying cause.
    pub fn other(msg: impl Into<String>) -> Self {
        BackendError::Other {
            msg: msg.into(),
            source: None,
        }
    }

    /// Builds an [`BackendError::Other`] that wraps an underlying cause, kept in
    /// the [`std::error::Error::source`] chain.
    pub fn with_source(
        msg: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        BackendError::Other {
            msg: msg.into(),
            source: Some(Cause::new(source)),
        }
    }
}

/// An opaque CAS token identifying a generation of an object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Version {
    pub token: Arc<str>,
}

impl Version {
    /// Wraps a token string.
    ///
    /// The token is stored behind an `Arc` so cloning a `Version` - which
    /// happens on every cached read and CAS comparison - is a refcount bump
    /// rather than a string copy.
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        Version {
            token: token.into(),
        }
    }

    /// Reports whether the version is unset.
    pub fn is_unset(&self) -> bool {
        self.token.is_empty()
    }
}

/// The contents and version of a read object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadReply {
    pub contents: Vec<u8>,
    pub version: Version,
}

/// A provider-issued continuation token for a paginated listing.
///
/// The token has no engine-level meaning. Callers may only retain it and pass it
/// back to the same backend with the same prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListCursor(Arc<str>);

impl ListCursor {
    /// Wraps the provider token returned by a backend implementation.
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        ListCursor(token.into())
    }

    /// Returns the provider token for forwarding to the underlying store.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A positive upper bound on the objects returned by one listing call.
pub type ListLimit = NonZeroUsize;

/// One page of object paths returned by [`Backend::list`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    /// Actual object paths matching the requested prefix.
    pub objects: Vec<String>,
    /// The opaque cursor for the next page, or `None` when traversal completed.
    pub next: Option<ListCursor>,
}

/// The contract with an object store (ADR-023).
///
/// The surface is content-CAS only: there are no metadata tags, and the opaque
/// [`Version`] is the sole conditional token. Backend futures are cancelled by
/// being dropped: wrap a call in `tokio::time::timeout` or `select!`.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Reads the full object.
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError>;

    /// Reads the object only if its version differs from `expected`; otherwise
    /// returns [`BackendError::Precondition`] to signal "not modified" (the
    /// caller's cached copy at `expected` is still current). Maps to a native
    /// conditional GET (`If-None-Match` / `ifGenerationNotMatch`), so a hot,
    /// unchanged object revalidates without transferring its body.
    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError>;

    /// Unconditionally writes (creates or overwrites) the object, returning its
    /// new version.
    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError>;

    /// Conditionally writes if the object exists and its version matches
    /// `expected`, returning the new version.
    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError>;

    /// Creates the object only if it does not already exist, returning its
    /// version.
    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError>;

    /// Unconditionally deletes the object.
    async fn delete(&self, path: &str) -> Result<(), BackendError>;

    /// Lists one page of object paths recursively under `prefix`.
    ///
    /// `prefix` is empty or ends in `/`. `cursor`, when present, must have been
    /// returned by this backend for the same prefix. Result order is unspecified
    /// and only `ListPage::next == None` means traversal is complete.
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError>;
}

/// Transparent delegation so any `Arc<B: Backend>` (including
/// `Arc<dyn Backend>`) is itself a `Backend`. Lets generic APIs like
/// `Database::open<B: Backend + 'static>(name, b)` accept a pre-erased
/// `Arc<dyn Backend>` (e.g. a middleware stack assembled in a test) without a
/// dedicated entry point.
#[async_trait]
impl<B: Backend + ?Sized + 'static> Backend for std::sync::Arc<B> {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        (**self).read(path).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        (**self).read_if_modified(path, expected).await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        (**self).write(path, value).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        (**self).write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        (**self).write_if_not_exists(path, value).await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        (**self).delete(path).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        (**self).list(prefix, cursor, limit).await
    }
}
