//! Amazon S3 backend for GlassDB (ADR-016, ADR-023).
//!
//! Each logical key maps to a single S3 object whose body holds the value.
//! Coordination is content CAS only: conditional writes use `If-Match` /
//! `If-None-Match` on the object ETag, and the opaque [`Version`] token is that
//! ETag (kept verbatim, quotes included). Conditional reads use `If-None-Match`.

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use glassdb_backend::{Backend, BackendError, Cause, ReadReply, Version};

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

/// Builds an [`S3Backend`], allowing the per-operation retry strategy to be
/// tuned independently of how the injected client was configured.
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

    /// Sets the maximum number of attempts per S3 operation (including the
    /// first), keeping the adaptive strategy.
    pub fn max_retry_attempts(mut self, n: u32) -> Self {
        self.retry = self.retry.with_max_attempts(n);
        self
    }

    /// Overrides the entire retry strategy applied to every S3 operation.
    pub fn retry_config(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    /// Disables backend-level retries (the `aws.NopRetryer{}` equivalent).
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
    // Shared across all operations via per-call config override, so the
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

    /// A config override carrying this backend's retryer, applied to every
    /// operation so it is independent of the client's own config.
    fn overrides(&self) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::default().retry_config(self.retry.clone())
    }

    /// Awaits an idempotent S3 operation and maps SDK errors. Used for reads
    /// and the conditional GET behind `read_if_modified` — never for the
    /// conditional-write loop, which classifies its own outcomes (see
    /// [`S3Backend::put`]). Because the request is idempotent, transient
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
            Ok(out) => PutAttempt::Ok(version_from_etag(out.e_tag())),
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
        let conditional = conds.if_match.is_some() || conds.if_none_match;

        if !conditional {
            // An unconditional overwrite is idempotent (re-applying the same
            // body is harmless), so the SDK's adaptive retryer may ride out
            // throttling/transient failures transparently.
            return match self
                .send_put(path, &value, &conds, self.retry.clone())
                .await
            {
                PutAttempt::Ok(version) => Ok(version),
                PutAttempt::Err(e) => Err(annotate("Write", path, *e)),
            };
        }

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
    Err(Box<SdkError<PutObjectError>>),
}

/// The optional conditional headers for a PutObject.
#[derive(Default)]
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
        // The ETag changes on every content write, so a conditional GET with
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

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        self.put(path, value, PutConds::default()).await
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

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        Self::run(
            "Delete",
            path,
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(path)
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await
        .map(|_| ())
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        let prefix = ensure_trailing_slash(dir_path);
        let mut token: Option<String> = None;
        let mut keys: Vec<String> = Vec::new();
        loop {
            let mut op = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .delimiter("/");
            if let Some(t) = &token {
                op = op.continuation_token(t);
            }
            let out = Self::run(
                "List",
                &prefix,
                op.customize().config_override(self.overrides()).send(),
            )
            .await?;
            for cp in out.common_prefixes() {
                if let Some(p) = cp.prefix() {
                    keys.push(p.to_string());
                }
            }
            for o in out.contents() {
                if let Some(k) = o.key() {
                    keys.push(k.to_string());
                }
            }
            if out.is_truncated() == Some(true) {
                token = out.next_continuation_token().map(str::to_string);
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        keys.sort();
        Ok(keys)
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
/// failures, and server errors that are not a throttle (`500`/`502`/`504`).
fn is_ambiguous<E>(e: &SdkError<E>) -> bool {
    if matches!(e, SdkError::TimeoutError(_) | SdkError::DispatchFailure(_)) {
        return true;
    }
    matches!(
        e.raw_response().map(|r| r.status().as_u16()),
        Some(500) | Some(502) | Some(504)
    )
}

/// Builds the in-doubt error returned when a conditional write's outcome cannot
/// be confirmed (a possibly-applied attempt followed by a precondition, or an
/// exhausted retry budget).
fn in_doubt(op: &str, path: &str) -> BackendError {
    BackendError::Unavailable(format!(
        "{op}({path}): conditional write outcome unknown after a lost or ambiguous attempt"
    ))
}

/// The delay before the given (zero-based) conflict retry: an exponential ramp
/// from 25ms, capped at one second.
fn conflict_backoff(attempt: u32) -> Duration {
    Duration::from_millis(25u64.saturating_mul(1u64 << attempt)).min(Duration::from_secs(1))
}

fn ensure_trailing_slash(a: &str) -> String {
    if a.ends_with('/') {
        a.to_string()
    } else {
        format!("{a}/")
    }
}
