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
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use glassdb_backend::{
    encode_writer_tag, Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId,
    LAST_WRITER_TAG,
};
use glassdb_concurr::Ctx;

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

    /// Awaits an S3 operation, cancelling on `ctx` and mapping SDK errors.
    async fn run<F, T, E>(ctx: &Ctx, op: &str, path: &str, fut: F) -> Result<T, BackendError>
    where
        F: Future<Output = Result<T, SdkError<E>>>,
        E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    {
        tokio::select! {
            _ = ctx.cancelled() => Err(BackendError::Cancelled),
            r = fut => r.map_err(|e| annotate(op, path, e)),
        }
    }

    async fn put(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
        conds: PutConds,
    ) -> Result<Metadata, BackendError> {
        if let Some(m) = &conds.if_match {
            if m.is_empty() {
                // An empty token can never match a stored ETag and S3 rejects an
                // empty If-Match header. Treat it as a failed precondition rather
                // than risk an unconditional overwrite.
                return Err(BackendError::Precondition);
            }
        }
        let payload = add_nonce(&value);
        let metadata: Option<HashMap<String, String>> = if tags.is_empty() {
            None
        } else {
            Some(tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        };

        // Two retry layers compose: the SDK retryer rides out throttling/5xx
        // within each PutObject, while this loop re-issues on 409
        // ConditionalRequestConflict (which the SDK does not retry).
        let mut attempt: u32 = 0;
        loop {
            let mut op = self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(path)
                .body(ByteStream::from(payload.clone()))
                .set_metadata(metadata.clone());
            if let Some(m) = &conds.if_match {
                op = op.if_match(m);
            }
            if conds.if_none_match {
                op = op.if_none_match("*");
            }
            let send = op.customize().config_override(self.overrides()).send();
            let res = tokio::select! {
                _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                r = send => r,
            };
            match res {
                Ok(out) => {
                    return Ok(Metadata {
                        tags,
                        version: version_from_etag(out.e_tag()),
                    })
                }
                Err(e) => {
                    if is_conflict(&e) && attempt < MAX_CONFLICT_RETRIES {
                        tokio::select! {
                            _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                            _ = tokio::time::sleep(conflict_backoff(attempt)) => {}
                        }
                        attempt += 1;
                        continue;
                    }
                    return Err(annotate("Write", path, e));
                }
            }
        }
    }
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
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        // With nonce-in-content the ETag changes on every write, so the
        // last-writer tag is the source of truth: compare it via HEAD before
        // downloading.
        let head = Self::run(
            ctx,
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
        self.read(ctx, path).await
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        let out = Self::run(
            ctx,
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

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        let out = Self::run(
            ctx,
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
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        // S3 has no metadata-only update, so the object must be re-uploaded.
        // The existing tags are preserved and the new tags overlaid on top. A
        // fresh nonce ensures the ETag changes so the conditional write is real
        // CAS.
        let out = Self::run(
            ctx,
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
            ctx,
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
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(ctx, path, value, tags, PutConds::default()).await
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(
            ctx,
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
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.put(
            ctx,
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

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        Self::run(
            ctx,
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

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        // S3 has no conditional delete, so this is a HEAD-then-DELETE with a
        // documented TOCTOU window covered by the transaction algorithm.
        let head = Self::run(
            ctx,
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
            ctx,
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

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
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
                ctx,
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
                return BackendError::Precondition
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
