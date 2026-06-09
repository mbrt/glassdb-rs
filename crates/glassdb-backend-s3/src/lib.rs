//! Amazon S3 backend for GlassDB. Ported from the Go `backend/s3` package.
//!
//! Each logical key maps to a single S3 object. The user value is stored in the
//! object body with an 8-byte random nonce prepended, and the lock/last-writer
//! tags are stored as S3 user metadata (`x-amz-meta-*`). The nonce guarantees a
//! fresh ETag on every PutObject, which restores compare-and-swap semantics for
//! metadata-only updates (S3 ETags are otherwise the MD5 of the content).

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStream;
use glassdb_backend::{
    Backend, BackendError, LAST_WRITER_TAG, Metadata, ReadReply, Tags, Version, WriterId,
    encode_writer_tag,
};

#[cfg(test)]
mod tests;

/// Number of random bytes prepended to every object body to force a unique
/// ETag on each write.
const NONCE_SIZE: usize = 8;

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

    /// Awaits an S3 operation and maps SDK errors. Cancellation is by
    /// dropping the surrounding future.
    async fn run<F, T, E>(op: &str, path: &str, fut: F) -> Result<T, BackendError>
    where
        F: Future<Output = Result<T, SdkError<E>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        fut.await.map_err(|e| annotate(op, path, e))
    }

    /// Issues a single PutObject with the given retryer, returning the new
    /// version on success or the raw SDK error to be classified by the caller.
    async fn send_put(
        &self,
        path: &str,
        payload: &[u8],
        metadata: &Option<HashMap<String, String>>,
        conds: &PutConds,
        retry: RetryConfig,
    ) -> PutAttempt {
        let mut op = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(payload.to_vec()))
            .set_metadata(metadata.clone());
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
        tags: Tags,
        conds: PutConds,
    ) -> Result<Metadata, BackendError> {
        if let Some(m) = &conds.if_match
            && m.is_empty()
        {
            // An empty token can never match a stored ETag and S3 rejects an
            // empty If-Match header. Treat it as a failed precondition rather
            // than risk an unconditional overwrite.
            return Err(BackendError::Precondition);
        }
        let payload = add_nonce(&value);
        let metadata: Option<HashMap<String, String>> = if tags.is_empty() {
            None
        } else {
            Some(tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        };
        let conditional = conds.if_match.is_some() || conds.if_none_match;

        if !conditional {
            // An unconditional overwrite is idempotent (the nonce only changes
            // the ETag), so the SDK's adaptive retryer may ride out
            // throttling/transient failures transparently: re-applying is
            // harmless.
            return match self
                .send_put(path, &payload, &metadata, &conds, self.retry.clone())
                .await
            {
                PutAttempt::Ok(version) => Ok(Metadata { tags, version }),
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
                .send_put(path, &payload, &metadata, &conds, RetryConfig::disabled())
                .await
            {
                PutAttempt::Ok(version) => return Ok(Metadata { tags, version }),
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
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        // With nonce-in-content the ETag changes on every write, so the
        // last-writer tag is the source of truth: compare it via HEAD before
        // downloading.
        let head = Self::run(
            "ReadIfModified",
            path,
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(path)
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await?;
        let current = head
            .metadata()
            .and_then(|m| m.get(LAST_WRITER_TAG))
            .map(String::as_str)
            .unwrap_or("");
        if current == encode_writer_tag(expected_writer) {
            return Err(BackendError::Precondition);
        }
        self.read(path).await
    }

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
        let tags = tags_from_meta(out.metadata());
        let stored = out
            .body
            .collect()
            .await
            .map_err(|e| BackendError::Other(format!("Read({path}): reading object body: {e}")))?
            .to_vec();
        Ok(ReadReply {
            contents: read_body(path, stored)?,
            version,
            tags,
        })
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        let out = Self::run(
            "GetMetadata",
            path,
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(path)
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await?;
        Ok(Metadata {
            tags: tags_from_meta(out.metadata()),
            version: version_from_etag(out.e_tag()),
        })
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        // S3 has no metadata-only update, so the object must be re-uploaded.
        // The existing tags are preserved and the new tags overlaid on top. A
        // fresh nonce ensures the ETag changes so the conditional write is real
        // CAS.
        let out = Self::run(
            "SetTagsIf",
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
        let mut merged = tags_from_meta(out.metadata());
        let stored = out.body.collect().await.map_err(|e| {
            BackendError::Other(format!("SetTagsIf({path}): reading object body: {e}"))
        })?;
        let value = read_body(path, stored.to_vec())?;
        for (k, v) in tags {
            merged.insert(k, v);
        }
        self.put(
            path,
            value,
            merged,
            PutConds {
                if_match: Some(expected.token.clone()),
                if_none_match: false,
            },
        )
        .await
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(path, value, tags, PutConds::default()).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(
            path,
            value,
            tags,
            PutConds {
                if_match: Some(expected.token.clone()),
                if_none_match: false,
            },
        )
        .await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(
            path,
            value,
            tags,
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

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        // S3 has no conditional delete, so this is a HEAD-then-DELETE with a
        // documented TOCTOU window covered by the transaction algorithm.
        let head = Self::run(
            "DeleteIf",
            path,
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(path)
                .customize()
                .config_override(self.overrides())
                .send(),
        )
        .await?;
        if &version_from_etag(head.e_tag()) != expected {
            return Err(BackendError::Precondition);
        }
        Self::run(
            "DeleteIf",
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

/// Prepends [`NONCE_SIZE`] random bytes to `value`.
fn add_nonce(value: &[u8]) -> Vec<u8> {
    let nonce: [u8; NONCE_SIZE] = rand::random();
    let mut buf = Vec::with_capacity(NONCE_SIZE + value.len());
    buf.extend_from_slice(&nonce);
    buf.extend_from_slice(value);
    buf
}

/// Strips the leading nonce from a stored object body.
fn read_body(path: &str, stored: Vec<u8>) -> Result<Vec<u8>, BackendError> {
    if stored.len() < NONCE_SIZE {
        return Err(BackendError::Other(format!(
            "Read({path}): object body too short ({} bytes) to contain nonce",
            stored.len()
        )));
    }
    Ok(stored[NONCE_SIZE..].to_vec())
}

/// Builds an opaque [`Version`] from an S3 ETag, kept verbatim (quotes
/// included).
fn version_from_etag(etag: Option<&str>) -> Version {
    match etag {
        Some(e) => Version::new(e),
        None => Version::default(),
    }
}

/// Converts S3 user metadata into [`Tags`].
fn tags_from_meta(meta: Option<&HashMap<String, String>>) -> Tags {
    match meta {
        Some(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        None => Tags::new(),
    }
}

/// Maps an S3 SDK error onto a [`BackendError`].
fn annotate<E>(op: &str, path: &str, e: SdkError<E>) -> BackendError
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
        Some(412) | Some(409) => BackendError::Precondition,
        _ => BackendError::Other(format!(
            "{op}({path}): code={code:?} status={status:?}: {e}"
        )),
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
