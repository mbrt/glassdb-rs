//! Object-storage backend abstraction (ADR-016, ADR-023, ADR-042).
//!
//! A [`Backend`] is a small, content-CAS-only contract over an object store:
//! reads (plain and version-conditional), conditional writes and deletion, and
//! list. Coordination state lives entirely in object **content**; there are no
//! metadata tags. Every object carries an opaque CAS [`Version`] (its
//! ETag/generation), which is the only token used for conditional reads and
//! mutations.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod conformance;
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
    /// conditional mutation this means the expected state was not current; for
    /// [`Backend::read_if_modified`] it means "not modified" — the caller's
    /// cached copy is still current.
    #[error("precondition failed")]
    Precondition,
    /// A listing cursor is invalid for the requested prefix or was rejected by
    /// the provider. Callers should restart that prefix from the beginning.
    #[error("invalid listing cursor")]
    InvalidCursor,
    /// The operation's outcome is unknown: the request may or may not have been
    /// applied. Returned when a call cannot be completed with a definitive
    /// answer — e.g. a conditional mutation whose acknowledgement was lost and
    /// whose retry then observed a precondition failure (so it cannot be told
    /// apart from a genuine conflict), or a sustained outage that exhausts the
    /// retry budget. Because a mutation's outcome can be in doubt, it must *not*
    /// be blindly retried; the caller decides how to proceed.
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

/// An opaque continuation token for a paginated listing.
///
/// The token has no engine-level meaning. Callers may only retain it and pass it
/// back to the same backend with the same prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListCursor(Arc<str>);

impl ListCursor {
    /// Reconstructs an opaque cursor previously returned by a backend.
    pub fn new(token: impl Into<Arc<str>>) -> Self {
        ListCursor(token.into())
    }

    /// Returns the opaque cursor representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A positive upper bound on the objects returned by one listing call.
pub type ListLimit = NonZeroUsize;

/// Validated arguments for listing one page of object paths.
///
/// The request borrows its prefix and cursor, so validation does not add an
/// allocation to listing. Construct it at the boundary where raw listing
/// arguments enter the backend contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRequest<'a> {
    prefix: &'a str,
    cursor: Option<&'a ListCursor>,
    provider_cursor: Option<&'a str>,
    limit: ListLimit,
}

impl<'a> ListRequest<'a> {
    /// Validates and groups the arguments for one listing call.
    pub fn new(
        prefix: &'a str,
        cursor: Option<&'a ListCursor>,
        limit: ListLimit,
    ) -> Result<Self, BackendError> {
        validate_list_prefix(prefix)?;
        let provider_cursor = cursor
            .map(|cursor| decode_list_cursor(prefix, cursor))
            .transpose()?;
        Ok(Self {
            prefix,
            cursor,
            provider_cursor,
            limit,
        })
    }

    /// Returns the recursive object-path prefix.
    pub fn prefix(&self) -> &str {
        self.prefix
    }

    /// Returns the continuation cursor, when listing a subsequent page.
    pub fn cursor(&self) -> Option<&ListCursor> {
        self.cursor
    }

    /// Returns the validated provider continuation token, when present.
    pub fn provider_cursor(&self) -> Option<&str> {
        self.provider_cursor
    }

    /// Returns the positive page-size limit.
    pub fn limit(&self) -> ListLimit {
        self.limit
    }
}

const LIST_CURSOR_PREFIX: &str = "glassdb-list-v1:";

/// Binds a provider continuation token to its listing prefix.
#[doc(hidden)]
pub fn bind_list_cursor(prefix: &str, provider_token: &str) -> Result<ListCursor, BackendError> {
    validate_list_prefix(prefix)?;
    if provider_token.is_empty() {
        return Err(BackendError::other(
            "list provider returned an empty continuation token",
        ));
    }
    Ok(ListCursor::new(format!(
        "{LIST_CURSOR_PREFIX}{}:{prefix}{provider_token}",
        prefix.len()
    )))
}

fn validate_list_prefix(prefix: &str) -> Result<(), BackendError> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Ok(())
    } else {
        Err(BackendError::other(format!(
            "list prefix must be empty or end in '/': {prefix:?}"
        )))
    }
}

fn decode_list_cursor<'a>(prefix: &str, cursor: &'a ListCursor) -> Result<&'a str, BackendError> {
    let encoded = cursor
        .as_str()
        .strip_prefix(LIST_CURSOR_PREFIX)
        .ok_or(BackendError::InvalidCursor)?;
    let (prefix_len, body) = encoded.split_once(':').ok_or(BackendError::InvalidCursor)?;
    let prefix_len = prefix_len
        .parse::<usize>()
        .map_err(|_| BackendError::InvalidCursor)?;
    let stored_prefix = body.get(..prefix_len).ok_or(BackendError::InvalidCursor)?;
    let provider_token = body
        .get(prefix_len..)
        .filter(|token| !token.is_empty())
        .ok_or(BackendError::InvalidCursor)?;
    if stored_prefix != prefix {
        return Err(BackendError::InvalidCursor);
    }
    Ok(provider_token)
}

/// One page of object paths returned by [`Backend::list_request`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    /// Actual object paths matching the requested prefix.
    pub objects: Vec<String>,
    /// The opaque cursor for the next page, or `None` when traversal completed.
    pub next: Option<ListCursor>,
}

/// The conditional-only contract with an object store (ADR-023, ADR-042).
///
/// The surface is content-CAS only: there are no metadata tags, and the opaque
/// [`Version`] is the sole conditional token. Single-object reads and
/// conditional mutations must be linearizable, including reads invoked after a
/// definitive mutation completes. Eventually consistent implementations are
/// not supported. For mutations,
/// [`BackendError::Unavailable`] is the only returned outcome that may have
/// applied; every other error guarantees the mutation did not apply. Backend
/// futures are cancelled by being dropped: wrap a call in
/// `tokio::time::timeout` or `select!`.
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

    /// Deletes the object only if its version matches `expected`.
    ///
    /// A missing object may be reported as [`BackendError::NotFound`] or as
    /// success; both mean the path has converged on absence. An ambiguous
    /// outcome is always [`BackendError::Unavailable`].
    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError>;

    /// Lists one page of object paths recursively.
    ///
    /// Its prefix is empty or ends in `/`. Its cursor, when present, must have been
    /// returned by this backend for the same prefix. Result order is unspecified
    /// and only `ListPage::next == None` means traversal is complete.
    async fn list_request(&self, request: ListRequest<'_>) -> Result<ListPage, BackendError>;
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

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        (**self).delete_if(path, expected).await
    }

    async fn list_request(&self, request: ListRequest<'_>) -> Result<ListPage, BackendError> {
        (**self).list_request(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_request_and_cursor_boundaries() {
        let one = ListLimit::new(1).unwrap();
        let largest = ListLimit::new(usize::MAX).unwrap();
        let root_cursor = bind_list_cursor("", "root-token").unwrap();
        let slash_cursor = bind_list_cursor("/", "slash-token").unwrap();
        let nested_cursor = bind_list_cursor("a/b/", "nested-token").unwrap();

        for (prefix, cursor, limit, provider_token) in [
            ("", None, one, None),
            ("", Some(&root_cursor), one, Some("root-token")),
            ("/", Some(&slash_cursor), one, Some("slash-token")),
            ("a/", None, largest, None),
            ("a/b/", Some(&nested_cursor), largest, Some("nested-token")),
        ] {
            let request = ListRequest::new(prefix, cursor, limit).unwrap();
            assert_eq!(request.prefix(), prefix);
            assert_eq!(request.cursor(), cursor);
            assert_eq!(
                request.provider_cursor(),
                provider_token,
                "prefix {prefix:?}"
            );
            assert_eq!(request.limit(), limit);
        }

        let malformed_cursor = ListCursor::new("opaque");
        for (prefix, cursor) in [("a", None), ("a/b", Some(&malformed_cursor))] {
            let error = ListRequest::new(prefix, cursor, one).unwrap_err();
            match error {
                BackendError::Other { msg, source } => {
                    assert_eq!(
                        msg,
                        format!("list prefix must be empty or end in '/': {prefix:?}")
                    );
                    assert!(source.is_none());
                }
                error => panic!("prefix {prefix:?} returned {error:?}"),
            }
        }

        let unicode_cursor = bind_list_cursor("é/", "provider-token").unwrap();
        assert_eq!(
            unicode_cursor.as_str(),
            "glassdb-list-v1:3:é/provider-token"
        );
        assert_eq!(
            ListRequest::new("é/", Some(&unicode_cursor), one)
                .unwrap()
                .provider_cursor(),
            Some("provider-token")
        );

        let wrong_prefix = bind_list_cursor("other/", "provider-token").unwrap();
        for (case, cursor) in [
            ("empty", ListCursor::new("")),
            ("raw provider token", ListCursor::new("provider-token")),
            (
                "unknown version",
                ListCursor::new("glassdb-list-v2:2:a/provider-token"),
            ),
            (
                "invalid length",
                ListCursor::new("glassdb-list-v1:x:a/provider-token"),
            ),
            (
                "truncated prefix",
                ListCursor::new("glassdb-list-v1:20:a/provider-token"),
            ),
            (
                "non-character boundary",
                ListCursor::new("glassdb-list-v1:1:é/provider-token"),
            ),
            (
                "empty provider token",
                ListCursor::new("glassdb-list-v1:2:a/"),
            ),
            ("wrong prefix", wrong_prefix),
        ] {
            let error = ListRequest::new("a/", Some(&cursor), one).unwrap_err();
            assert!(
                matches!(error, BackendError::InvalidCursor),
                "{case} returned {error:?}"
            );
        }

        assert!(matches!(
            bind_list_cursor("a/", ""),
            Err(BackendError::Other { .. })
        ));
        assert!(ListLimit::new(0).is_none());
    }
}
