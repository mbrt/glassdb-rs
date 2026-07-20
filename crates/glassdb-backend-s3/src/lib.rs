//! Amazon S3 backend for GlassDB (ADR-016, ADR-023, ADR-042).
//!
//! Each logical key maps to a single S3 object whose body holds the value.
//! Coordination is content CAS only: conditional writes use `If-Match` /
//! `If-None-Match` and conditional deletion uses `If-Match` on the object ETag.
//! The opaque [`Version`] token is that ETag (kept verbatim, quotes included).
//! Conditional reads use `If-None-Match`.

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use glassdb_backend::{
    Backend, BackendError, Cause, ListCursor, ListLimit, ListPage, ReadReply, Version,
};

const MAX_LIST_PAGE_SIZE: usize = 1_000;

// The in-process fake S3 server is compiled for this crate's own tests and,
// when the `fake-server` feature is on, exposed as a reusable component (used by
// the benchmarks to drive the real S3 transport against an in-memory server).
#[cfg(any(test, feature = "fake-server"))]
mod fake_server;
#[cfg(feature = "fake-server")]
pub use fake_server::{FakeS3, FakeS3Options};

mod dns;
#[cfg(test)]
mod tests;
mod tuned_http;

pub use tuned_http::tuned_http_client;

/// Bounds how many extra times a conditional PutObject is retried after S3
/// reports a 409 `ConditionalRequestConflict`.
const MAX_CONFLICT_RETRIES: u32 = 5;

/// Default number of attempts per S3 operation. Intentionally higher than the
/// SDK default (3) because S3 throttles a hot prefix with `503 SlowDown` while
/// it reactively splits a partition, a window that can outlast a few attempts.
const DEFAULT_MAX_ATTEMPTS: u32 = 10;

/// Builds an [`S3Backend`], allowing the idempotent-operation retry strategy to
/// be tuned independently of how the injected client was configured.
pub struct Builder {
    client: Client,
    bucket: String,
    retry: RetryConfig,
}

impl Builder {
    /// Starts building a backend over `client` and `bucket` with the default
    /// adaptive retryer (max attempts [`DEFAULT_MAX_ATTEMPTS`]).
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Builder {
            client,
            bucket: bucket.into(),
            retry: RetryConfig::adaptive().with_max_attempts(DEFAULT_MAX_ATTEMPTS),
        }
    }

    /// Sets the maximum number of attempts for idempotent S3 operations
    /// (including the first), keeping the adaptive strategy. Conditional
    /// mutations own their retry and in-doubt classification.
    pub fn max_retry_attempts(mut self, n: u32) -> Self {
        self.retry = self.retry.with_max_attempts(n);
        self
    }

    /// Overrides the retry strategy applied to idempotent S3 operations.
    pub fn retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Disables retries for idempotent operations (the `aws.NopRetryer{}`
    /// equivalent). Conditional mutations retain their outcome-aware policy.
    pub fn disable_retries(mut self) -> Self {
        self.retry = RetryConfig::disabled();
        self
    }

    /// Finishes building the backend.
    pub fn build(self) -> S3Backend {
        S3Backend {
            client: self.client,
            bucket: self.bucket,
            retry: self.retry,
        }
    }
}

/// A [`Backend`] implemented on top of Amazon S3.
pub struct S3Backend {
    client: Client,
    bucket: String,
    // Applied to idempotent operations via per-call config override, so their
    // retryer is independent of how the client was constructed.
    retry: RetryConfig,
}

impl S3Backend {
    /// Creates a backend over the given S3 `client` and `bucket` with the
    /// default adaptive retryer.
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Builder::new(client, bucket).build()
    }

    /// Starts a [`Builder`] for finer control over retries.
    pub fn builder(client: Client, bucket: impl Into<String>) -> Builder {
        Builder::new(client, bucket)
    }

    /// A config override carrying this backend's retryer for idempotent
    /// operations, independent of the client's own config.
    fn overrides(&self) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::default().retry_config(self.retry.clone())
    }

    /// Awaits an idempotent S3 operation and maps SDK errors. Used for reads
    /// and the conditional GET behind `read_if_modified` — never for
    /// conditional mutations, which classify their own outcomes. Because the
    /// request is idempotent, transient
    /// failures are surfaced as `Unavailable` (retryable) via [`annotate_read`].
    /// Cancellation is by dropping the surrounding future.
    async fn run<F, T, E>(op: &'static str, path: &str, fut: F) -> Result<T, BackendError>
    where
        F: Future<Output = Result<T, SdkError<E>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        fut.await.map_err(|e| annotate_read(op, path, e))
    }

    /// Issues a single PutObject with the given retryer, returning the new
    /// version on success or the raw SDK error to be classified by the caller.
    async fn send_put(
        &self,
        path: &str,
        payload: &[u8],
        conds: &PutConds,
        retry: RetryConfig,
    ) -> PutAttempt {
        let mut op = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(payload.to_vec()));
        if let Some(m) = &conds.if_match {
            op = op.if_match(m);
        }
        if conds.if_none_match {
            op = op.if_none_match("*");
        }
        let cfg = aws_sdk_s3::config::Builder::default().retry_config(retry);
        match op.customize().config_override(cfg).send().await {
            Ok(out) => match out.e_tag().filter(|etag| !etag.is_empty()) {
                Some(etag) => PutAttempt::Ok(Version::new(etag)),
                None => PutAttempt::AppliedWithoutVersion,
            },
            Err(e) => PutAttempt::Err(Box::new(e)),
        }
    }

    async fn put(
        &self,
        path: &str,
        value: Vec<u8>,
        conds: PutConds,
    ) -> Result<Version, BackendError> {
        if let Some(m) = &conds.if_match
            && m.is_empty()
        {
            // An empty token can never match a stored ETag and S3 rejects an
            // empty If-Match header. Treat it as a failed precondition rather
            // than risk an unconditional overwrite.
            return Err(BackendError::Precondition);
        }
        debug_assert!(conds.if_match.is_some() || conds.if_none_match);

        // A conditional write is NOT idempotent under retry: if an attempt lands
        // but its acknowledgement is lost, a re-send sees its own write and
        // returns a precondition failure indistinguishable from a real conflict.
        // We therefore disable the SDK retryer and own the loop, so we can taint
        // such a precondition as in-doubt instead of reporting a confident
        // conflict (which the engine would retry into a double-apply). See
        // ADR-009.
        let mut lost = false;
        let mut attempt: u32 = 0;
        loop {
            let e = match self
                .send_put(path, &value, &conds, RetryConfig::disabled())
                .await
            {
                PutAttempt::Ok(version) => {
                    return Ok(version);
                }
                PutAttempt::AppliedWithoutVersion => return Err(in_doubt("Write", path)),
                PutAttempt::Err(e) => *e,
            };

            if is_precondition(&e) {
                // If an earlier attempt may have applied, this precondition could
                // be our own landed write rather than a competitor's: in doubt.
                return Err(if lost {
                    in_doubt("Write", path)
                } else {
                    BackendError::Precondition
                });
            }
            // 409 ConditionalRequestConflict: concurrent conditional writes
            // raced; this one was not applied, so retrying it is safe and does
            // not taint a later precondition.
            if is_conflict(&e) && attempt < MAX_CONFLICT_RETRIES {
                tokio::time::sleep(conflict_backoff(attempt)).await;
                attempt += 1;
                continue;
            }
            let ambiguous = is_ambiguous(&e);
            if (ambiguous || is_throttle(&e)) && attempt < DEFAULT_MAX_ATTEMPTS {
                // An ambiguous attempt (timeout/dispatch/5xx) may have applied;
                // a throttle (503/429) was rejected before applying and is safe.
                lost = lost || ambiguous;
                tokio::time::sleep(conflict_backoff(attempt)).await;
                attempt += 1;
                continue;
            }
            // Terminal error, or the retry budget is exhausted. If any attempt
            // may have applied, the final outcome is unknown.
            return Err(if lost {
                in_doubt("Write", path)
            } else {
                annotate("Write", path, e)
            });
        }
    }
}

/// The outcome of a single PutObject attempt.
enum PutAttempt {
    Ok(Version),
    AppliedWithoutVersion,
    Err(Box<SdkError<PutObjectError>>),
}

/// The optional conditional headers for a PutObject.
struct PutConds {
    if_match: Option<String>,
    if_none_match: bool,
}

#[async_trait]
impl Backend for S3Backend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let out = Self::run(
            "Read",
            path,
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(path)
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await?;
        let version = version_from_etag(out.e_tag());
        let stored = out
            .body
            .collect()
            .await
            .map_err(|e| {
                BackendError::with_source(format!("Read({path}): reading object body"), e)
            })?
            .to_vec();
        Ok(ReadReply {
            contents: stored,
            version,
        })
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        if expected.is_unset() {
            return self.read(path).await;
        }
        // The ETag identifies the content state, so a conditional GET with
        // `If-None-Match` revalidates without transferring the body: an
        // unchanged object answers `304 Not Modified`, mapped to `Precondition`
        // by `annotate` (see the 304 arm).
        let out = Self::run(
            "ReadIfModified",
            path,
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(path)
                .if_none_match(expected.token.as_ref())
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await?;
        let version = version_from_etag(out.e_tag());
        let stored = out
            .body
            .collect()
            .await
            .map_err(|e| {
                BackendError::with_source(format!("ReadIfModified({path}): reading object body"), e)
            })?
            .to_vec();
        Ok(ReadReply {
            contents: stored,
            version,
        })
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.put(
            path,
            value,
            PutConds {
                if_match: Some(expected.token.to_string()),
                if_none_match: false,
            },
        )
        .await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.put(
            path,
            value,
            PutConds {
                if_match: None,
                if_none_match: true,
            },
        )
        .await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        if expected.is_unset() {
            return Err(BackendError::Precondition);
        }
        let retry = aws_sdk_s3::config::Builder::default().retry_config(RetryConfig::disabled());
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .if_match(expected.token.to_string())
            .customize()
            .config_override(retry)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_ambiguous(&error) || is_throttle(&error) => {
                Err(in_doubt("DeleteIf", path))
            }
            Err(error) => Err(annotate("DeleteIf", path, error)),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        validate_list_prefix(prefix)?;
        let max_keys = i32::try_from(limit.get().min(MAX_LIST_PAGE_SIZE)).unwrap();
        let mut op = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .max_keys(max_keys);
        if let Some(cursor) = cursor {
            op = op.continuation_token(cursor.as_str());
        }
        let out = op
            .customize()
            .config_override(self.overrides())
            .send()
            .await
            .map_err(|e| annotate_list(prefix, cursor.is_some(), e))?;
        let objects = out
            .contents()
            .iter()
            .filter_map(|object| object.key().map(str::to_string))
            .collect();
        let next = if out.is_truncated() == Some(true) {
            let token = out
                .next_continuation_token()
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    BackendError::other(format!(
                        "List({prefix}): truncated response has no continuation token"
                    ))
                })?;
            Some(ListCursor::new(token))
        } else {
            None
        };
        Ok(ListPage { objects, next })
    }
}

/// Builds an opaque [`Version`] from an S3 ETag, kept verbatim (quotes
/// included).
fn version_from_etag(etag: Option<&str>) -> Version {
    match etag {
        Some(e) => Version::new(e),
        None => Version::default(),
    }
}

/// A structured diagnostic for an S3 request that failed without mapping to a
/// dedicated [`BackendError`] classification.
///
/// The request coordinates are kept as typed fields rather than interpolated
/// ad-hoc at each call site: the failure has a single `Display` definition,
/// renders its structure under `{:?}`, and preserves the SDK error with its
/// full `source()` chain as the underlying cause.
#[derive(Debug, thiserror::Error)]
#[error("{op}({path}): code={code:?} status={status:?}")]
struct S3RequestError {
    op: &'static str,
    path: String,
    code: Option<String>,
    status: Option<u16>,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl S3RequestError {
    /// Flattens the structured S3 error into the shared catch-all: the message
    /// is the structured `Display`, and the typed error (with the SDK error in
    /// its chain) is kept via [`std::error::Error::source`].
    fn into_backend_error(self) -> BackendError {
        // This is an inherent method rather than a `From` impl on purpose:
        // adding another `From<_> for BackendError` would make `?`/`Ok(())`
        // inference ambiguous at call sites that rely on `BackendError` being
        // the only inferable error type.
        BackendError::Other {
            msg: self.to_string(),
            source: Some(Cause::new(self)),
        }
    }
}

/// Maps an S3 SDK error from an *idempotent* request (`read` and the
/// conditional GET behind `read_if_modified`) onto a [`BackendError`].
///
/// A transient failure — throttle (`503`/`429`), timeout, dispatch failure, or
/// `5xx` — is always safe to retry on an idempotent request (ADR-009), so it is
/// classified as `Unavailable` (retryable in place by the engine) rather than a
/// generic `Other`. Definitive outcomes (404 -> `NotFound`, 412/409 ->
/// `Precondition`) keep their meaning via [`annotate`]. This is intentionally
/// distinct from the conditional-write path, which must not treat a transient
/// failure that never landed as in-doubt.
fn annotate_read<E>(op: &'static str, path: &str, e: SdkError<E>) -> BackendError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    if is_throttle(&e) || is_ambiguous(&e) {
        return BackendError::Unavailable(format!(
            "{op}({path}): transient backend failure, retryable"
        ));
    }
    annotate(op, path, e)
}

/// Maps a paginated listing failure, distinguishing a rejected continuation
/// token so the scanner can restart only this prefix.
fn annotate_list<E>(prefix: &str, had_cursor: bool, e: SdkError<E>) -> BackendError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let invalid_code = matches!(
        e.code(),
        Some("InvalidArgument" | "InvalidToken" | "InvalidContinuationToken")
    );
    let bad_request = e.raw_response().map(|r| r.status().as_u16()) == Some(400);
    if had_cursor && (invalid_code || bad_request) {
        return BackendError::InvalidCursor;
    }
    annotate_read("List", prefix, e)
}

/// Maps an S3 SDK error onto a [`BackendError`].
fn annotate<E>(op: &'static str, path: &str, e: SdkError<E>) -> BackendError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let code = e.code().map(str::to_string);
    let status = e.raw_response().map(|r| r.status().as_u16());
    if let Some(c) = code.as_deref() {
        match c {
            "PreconditionFailed" | "ConditionalRequestConflict" => {
                return BackendError::Precondition;
            }
            "NoSuchKey" | "NotFound" | "NoSuchBucket" => return BackendError::NotFound,
            _ => {}
        }
    }
    match status {
        Some(404) => BackendError::NotFound,
        // 304 Not Modified answers a conditional GET (`read_if_modified`):
        // the caller's cached copy is still current.
        Some(304) => BackendError::Precondition,
        Some(412) | Some(409) => BackendError::Precondition,
        _ => S3RequestError {
            op,
            path: path.to_string(),
            code,
            status,
            source: Box::new(e),
        }
        .into_backend_error(),
    }
}

/// Reports whether `e` is a 409 `ConditionalRequestConflict`, which S3 returns
/// when concurrent conditional writes race. Distinct from a 412 and retryable.
fn is_conflict<E>(e: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    if e.code() == Some("ConditionalRequestConflict") {
        return true;
    }
    e.raw_response().map(|r| r.status().as_u16()) == Some(409)
}

/// Reports whether `e` is a 412 precondition failure (an `If-Match`/
/// `If-None-Match` that did not hold). Kept distinct from a 409 conflict.
fn is_precondition<E>(e: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    if e.code() == Some("PreconditionFailed") {
        return true;
    }
    e.raw_response().map(|r| r.status().as_u16()) == Some(412)
}

/// Reports whether `e` is a throttle (`503 SlowDown` / `429`). The request was
/// rejected *before* being applied, so retrying it does not risk a double-apply.
fn is_throttle<E>(e: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    if matches!(e.code(), Some("SlowDown") | Some("ThrottlingException")) {
        return true;
    }
    matches!(
        e.raw_response().map(|r| r.status().as_u16()),
        Some(503) | Some(429)
    )
}

/// Reports whether `e`'s outcome is ambiguous: the request may have reached S3
/// and been applied before the failure. Covers transport timeouts, dispatch
/// failures, undecodable responses, and server errors that are not a throttle.
fn is_ambiguous<E>(e: &SdkError<E>) -> bool {
    if matches!(
        e,
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ResponseError(_)
    ) {
        return true;
    }
    e.raw_response()
        .map(|response| response.status().as_u16())
        .is_some_and(|status| (500..=599).contains(&status) && status != 503)
}

/// Builds the in-doubt error returned when a conditional mutation's outcome
/// cannot be confirmed (a possibly-applied attempt followed by a precondition,
/// or an exhausted retry budget).
fn in_doubt(op: &str, path: &str) -> BackendError {
    BackendError::Unavailable(format!(
        "{op}({path}): conditional mutation outcome unknown after a lost or ambiguous attempt"
    ))
}

/// The delay before the given (zero-based) conflict retry: an exponential ramp
/// from 25ms, capped at one second.
fn conflict_backoff(attempt: u32) -> Duration {
    Duration::from_millis(25u64.saturating_mul(1u64 << attempt)).min(Duration::from_secs(1))
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
