//! Object-storage backend abstraction. Ported from the Go `backend` package.
//!
//! A [`Backend`] provides conditional reads/writes and listing over an object
//! store. Lock state and the last-writer are kept in object [`Tags`]; every
//! object carries an opaque CAS [`Version`].

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use glassdb_concurr::Ctx;

pub mod memory;
pub mod middleware;
mod stats;

pub use stats::{BackendStats, StatsBackend};

/// The tag key recording the transaction ID of the most recent writer.
pub const LAST_WRITER_TAG: &str = "last-writer";

/// Errors returned by backend operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BackendError {
    /// The object does not exist.
    #[error("object not found")]
    NotFound,
    /// A conditional operation's precondition failed (version mismatch).
    #[error("precondition failed")]
    Precondition,
    /// The operation's context was cancelled.
    #[error("context canceled")]
    Cancelled,
    /// The operation's outcome is unknown: the request may or may not have been
    /// applied. Returned when a call cannot be completed with a definitive
    /// answer — e.g. a conditional write whose acknowledgement was lost and
    /// whose retry then observed a precondition failure (so it cannot be told
    /// apart from a genuine conflict), or a sustained outage that exhausts the
    /// retry budget. Because the outcome is in doubt, a non-idempotent operation
    /// must *not* be blindly retried; the caller decides how to proceed.
    #[error("storage outcome unknown (in doubt): {0}")]
    Unavailable(String),
    /// Any other backend error.
    #[error("{0}")]
    Other(String),
}

impl BackendError {
    /// Reports whether this is a not-found error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, BackendError::NotFound)
    }

    /// Reports whether this is a precondition-failed error.
    pub fn is_precondition(&self) -> bool {
        matches!(self, BackendError::Precondition)
    }

    /// Reports whether the operation's outcome is unknown (in doubt). Such an
    /// operation may or may not have taken effect and must not be blindly
    /// retried if it is not idempotent.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, BackendError::Unavailable(_))
    }
}

/// Key-value metadata pairs associated with an object. A `BTreeMap` is used so
/// iteration order is deterministic.
pub type Tags = BTreeMap<String, String>;

/// An opaque CAS token identifying a generation of an object.
///
/// The token is held behind an `Arc<str>` so propagating a version - which
/// happens on every read as it flows backend -> cache -> `ReadValue` -> the
/// transaction's read set - is a refcount bump rather than a fresh `String`
/// copy of the token each hop.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Version {
    pub token: Arc<str>,
}

impl Version {
    /// Wraps a token string.
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        Version {
            token: token.into(),
        }
    }

    /// Reports whether the version is unset.
    pub fn is_null(&self) -> bool {
        self.token.is_empty()
    }
}

/// The opaque identifier of the transaction that last wrote an object. Mirrors
/// the Go `WriterID`; kept distinct from `data::TxId` so this crate stays
/// independent of internal types.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriterId(Vec<u8>);

impl WriterId {
    /// Wraps raw bytes.
    pub fn new(b: impl Into<Vec<u8>>) -> Self {
        WriterId(b.into())
    }

    /// Reports whether the writer is empty (nil).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Encodes a [`WriterId`] into the string form used in object tags. Returns the
/// empty string when the writer is nil.
pub fn encode_writer_tag(w: &WriterId) -> String {
    if w.0.is_empty() {
        String::new()
    } else {
        base64::engine::general_purpose::URL_SAFE.encode(&w.0)
    }
}

/// The contents, version, and tags of a read object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadReply {
    pub contents: Vec<u8>,
    pub version: Version,
    pub tags: Tags,
}

/// The tags and version of an object (no contents).
///
/// `tags` is shared via `Arc` so a backend that already holds the tag map (e.g.
/// the in-memory backend, and the read-through cache) can hand it out on
/// `get_metadata`/read with a refcount bump instead of deep-copying the
/// `BTreeMap` and all its key/value `String`s on every validation and lock-info
/// parse. The map is immutable once produced; backends that mutate tags do so
/// copy-on-write (`Arc::make_mut`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Metadata {
    pub tags: Arc<Tags>,
    pub version: Version,
}

/// The contract with an object store. All methods take a [`Ctx`] for
/// cancellation.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Returns the object only if its `last-writer` tag differs from
    /// `expected_writer`; otherwise returns [`BackendError::Precondition`].
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError>;

    /// Reads the full object.
    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError>;

    /// Returns the object's tags and version.
    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError>;

    /// Conditionally merges `tags` if the object's version matches `expected`.
    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError>;

    /// Unconditionally writes (creates or overwrites) the object.
    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError>;

    /// Conditionally writes if the object exists and its version matches.
    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError>;

    /// Creates the object only if it does not already exist.
    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError>;

    /// Unconditionally deletes the object.
    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError>;

    /// Conditionally deletes if the object's version matches `expected`.
    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError>;

    /// Lists object paths under `dir_path` (separator `/`), lexicographically.
    /// Immediate sub-directory prefixes are returned, not their contents.
    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError>;
}
