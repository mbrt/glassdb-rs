//! Google Cloud Storage backend for GlassDB. Ported from the Go `backend/gcs`
//! package.
//!
//! Each logical key maps to a single GCS object. The user value is stored as
//! the object body and the lock/last-writer tags are stored as object custom
//! metadata. GCS provides native compare-and-swap through object generation and
//! metageneration preconditions, so the opaque [`Version`] token encodes both
//! as `"{generation}/{metageneration}"`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use glassdb_backend::{
    Backend, BackendError, Cause, LAST_WRITER_TAG, Metadata, ReadReply, Tags, Version, WriterId,
    encode_writer_tag,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::Deserialize;

#[cfg(test)]
mod tests;

/// OAuth scope granting full control over GCS objects.
const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.full_control";

/// Default endpoint for the GCS JSON API.
const DEFAULT_ENDPOINT: &str = "https://storage.googleapis.com";

/// MIME boundary used for the multipart upload body.
const BOUNDARY: &str = "glassdb_gcs_multipart_boundary";

/// Bounds how many times a read retries when the object is rewritten between
/// fetching its metadata and downloading its body.
const MAX_READ_RETRIES: usize = 3;

/// Object-name percent-encoding set: everything that is not an RFC 3986
/// unreserved character, which crucially encodes `/` as `%2F`.
const NAME_ENCODE: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Provides OAuth bearer tokens for authenticating against GCS.
#[async_trait]
pub trait TokenProvider: Send + Sync {
    /// Returns a valid bearer token (without the `Bearer ` prefix).
    async fn token(&self) -> Result<String, BackendError>;
}

/// Application Default Credentials provider backed by `gcp_auth`, initialized
/// lazily on first use so that constructing a backend never blocks or fails.
struct AdcTokenProvider {
    cell: tokio::sync::OnceCell<Arc<dyn gcp_auth::TokenProvider>>,
}

#[async_trait]
impl TokenProvider for AdcTokenProvider {
    async fn token(&self) -> Result<String, BackendError> {
        let provider = self
            .cell
            .get_or_try_init(|| async {
                gcp_auth::provider()
                    .await
                    .map_err(|e| BackendError::with_source("gcs auth provider", e))
            })
            .await?;
        let token = provider
            .token(&[GCS_SCOPE])
            .await
            .map_err(|e| BackendError::with_source("gcs auth token", e))?;
        Ok(token.as_str().to_string())
    }
}

/// A [`Backend`] implemented on top of the Google Cloud Storage JSON API.
pub struct GcsBackend {
    http: Client,
    base_url: String,
    bucket: String,
    auth: Option<Arc<dyn TokenProvider>>,
}

impl GcsBackend {
    /// Creates a backend over the real GCS endpoint, authenticating with
    /// Application Default Credentials (resolved lazily on first request).
    pub fn new(bucket: impl Into<String>) -> Self {
        let auth: Arc<dyn TokenProvider> = Arc::new(AdcTokenProvider {
            cell: tokio::sync::OnceCell::new(),
        });
        GcsBackend {
            http: Client::new(),
            base_url: DEFAULT_ENDPOINT.to_string(),
            bucket: bucket.into(),
            auth: Some(auth),
        }
    }

    /// Creates an unauthenticated backend pointed at a custom endpoint, for the
    /// GCS emulator or an in-process fake.
    pub fn with_endpoint(bucket: impl Into<String>, base_url: impl Into<String>) -> Self {
        GcsBackend {
            http: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            bucket: bucket.into(),
            auth: None,
        }
    }

    /// Creates a backend at a custom endpoint authenticated with a caller
    /// supplied token provider.
    pub fn with_token_provider(
        bucket: impl Into<String>,
        base_url: impl Into<String>,
        provider: Arc<dyn TokenProvider>,
    ) -> Self {
        GcsBackend {
            http: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            bucket: bucket.into(),
            auth: Some(provider),
        }
    }

    fn object_url(&self, path: &str) -> String {
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.base_url,
            self.bucket,
            utf8_percent_encode(path, NAME_ENCODE)
        )
    }

    fn objects_url(&self) -> String {
        format!("{}/storage/v1/b/{}/o", self.base_url, self.bucket)
    }

    fn upload_url(&self) -> String {
        format!("{}/upload/storage/v1/b/{}/o", self.base_url, self.bucket)
    }

    /// Attaches the bearer token to `rb` when the backend is authenticated.
    async fn authorize(&self, rb: RequestBuilder) -> Result<RequestBuilder, BackendError> {
        match &self.auth {
            Some(a) => Ok(rb.bearer_auth(a.token().await?)),
            None => Ok(rb),
        }
    }

    /// Sends `rb`. Cancellation is by dropping the surrounding future.
    ///
    /// Only idempotent operations go through `send` (reads, `get_metadata`,
    /// unconditional write/delete, and list); conditional writes use
    /// [`Self::send_conditional`]. A transport failure on an idempotent request
    /// is therefore always safe to retry, so it is reported as `Unavailable`
    /// rather than a generic `Other` (ADR-009), letting the engine recover a
    /// transient outage in place.
    async fn send(&self, rb: RequestBuilder) -> Result<reqwest::Response, BackendError> {
        let rb = self.authorize(rb).await?;
        rb.send()
            .await
            .map_err(|e| BackendError::Unavailable(format!("gcs request transport failure: {e}")))
    }

    /// Sends a *conditional* request and maps its outcome with the in-doubt
    /// contract (ADR-009). GCS applies conditional writes atomically and this
    /// backend does not retry them, so a clean `412`/`409` means the write did
    /// not take effect (a genuine `Precondition`). But a transport error or a
    /// `5xx` leaves the outcome unknown — the write may have landed before the
    /// failure — so it is reported as `Unavailable` rather than a confident error
    /// or a generic `Other`, ensuring the engine never retries it into a
    /// double-apply. An authentication failure happens before the request is
    /// sent, so it is not in-doubt.
    async fn send_conditional(
        &self,
        rb: RequestBuilder,
        op: &'static str,
        path: &str,
    ) -> Result<reqwest::Response, BackendError> {
        let rb = self.authorize(rb).await?;
        let resp = match rb.send().await {
            Ok(resp) => resp,
            // No response at all: the request may or may not have been applied.
            Err(e) => {
                return Err(BackendError::Unavailable(format!(
                    "{op}({path}): request failed, outcome unknown: {e}"
                )));
            }
        };
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        Err(check_conditional_status(status, op, path))
    }

    /// Fetches an object's metadata resource.
    async fn attrs(&self, path: &str) -> Result<ObjectResource, BackendError> {
        let rb = self
            .http
            .get(self.object_url(path))
            .query(&[("alt", "json")]);
        let resp = self.send(rb).await?;
        check_status(resp.status(), "GetMetadata", path)?;
        parse_json(resp, "GetMetadata", path).await
    }

    /// Downloads an object body, pinned to the generation in `attrs`. If the
    /// object is rewritten in between, the metadata is refetched and the read
    /// retried a bounded number of times.
    async fn read_from_attrs(
        &self,
        path: &str,
        mut attrs: ObjectResource,
    ) -> Result<ReadReply, BackendError> {
        for _ in 0..MAX_READ_RETRIES {
            let generation = attrs.generation.clone().unwrap_or_default();
            let rb = self
                .http
                .get(self.object_url(path))
                .query(&[("alt", "media"), ("ifGenerationMatch", generation.as_str())]);
            let resp = self.send(rb).await?;
            let status = resp.status();
            if status == StatusCode::PRECONDITION_FAILED {
                attrs = self.attrs(path).await?;
                continue;
            }
            check_status(status, "Read", path)?;
            let contents = resp
                .bytes()
                .await
                .map_err(|e| BackendError::with_source(format!("Read({path}): reading body"), e))?
                .to_vec();
            return Ok(ReadReply {
                contents,
                version: attrs.version(),
                tags: attrs.tags(),
            });
        }
        Err(BackendError::other(format!(
            "Read({path}): too many concurrent writes during read"
        )))
    }

    /// Uploads `value` and `tags` as a multipart insert with the given
    /// preconditions.
    async fn upload(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
        conds: WriteConds,
    ) -> Result<Metadata, BackendError> {
        let conditional =
            conds.if_generation_match.is_some() || conds.if_metageneration_match.is_some();
        let body = multipart_body(&object_metadata_json(path, &tags), &value);
        let mut query: Vec<(&str, String)> = vec![("uploadType", "multipart".to_string())];
        conds.apply(&mut query);
        let rb = self
            .http
            .post(self.upload_url())
            .query(&query)
            .header(
                CONTENT_TYPE,
                format!("multipart/related; boundary={BOUNDARY}"),
            )
            .body(body);
        let resp = if conditional {
            self.send_conditional(rb, "Write", path).await?
        } else {
            let resp = self.send(rb).await?;
            check_status(resp.status(), "Write", path)?;
            resp
        };
        let obj: ObjectResource = parse_json(resp, "Write", path).await?;
        Ok(Metadata {
            tags: Arc::new(tags),
            version: obj.version(),
        })
    }
}

/// Conditional-write preconditions translated into query parameters.
#[derive(Default)]
struct WriteConds {
    if_generation_match: Option<String>,
    if_metageneration_match: Option<String>,
}

impl WriteConds {
    fn apply(&self, query: &mut Vec<(&'static str, String)>) {
        if let Some(g) = &self.if_generation_match {
            query.push(("ifGenerationMatch", g.clone()));
        }
        if let Some(m) = &self.if_metageneration_match {
            query.push(("ifMetagenerationMatch", m.clone()));
        }
    }
}

#[async_trait]
impl Backend for GcsBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        let attrs = self.attrs(path).await?;
        let current = attrs
            .tags()
            .get(LAST_WRITER_TAG)
            .cloned()
            .unwrap_or_default();
        if current == encode_writer_tag(expected_writer) {
            return Err(BackendError::Precondition);
        }
        self.read_from_attrs(path, attrs).await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let attrs = self.attrs(path).await?;
        self.read_from_attrs(path, attrs).await
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        Ok(self.attrs(path).await?.metadata())
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let (generation, metageneration) = parse_token(expected)?;
        // GCS replaces the entire metadata map on update, so the new tags are
        // merged onto the current set to preserve the last-writer tag (the
        // locker only sends the lock tags here).
        let attrs = self.attrs(path).await?;
        let mut merged = attrs.tags();
        for (k, v) in tags {
            merged.insert(k, v);
        }
        let rb = self
            .http
            .patch(self.object_url(path))
            .query(&[
                ("ifGenerationMatch", generation.as_str()),
                ("ifMetagenerationMatch", metageneration.as_str()),
            ])
            .header(CONTENT_TYPE, "application/json")
            .body(metadata_patch_json(&merged));
        let resp = self.send_conditional(rb, "SetTagsIf", path).await?;
        let obj: ObjectResource = parse_json(resp, "SetTagsIf", path).await?;
        Ok(obj.metadata())
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.upload(path, value, tags, WriteConds::default()).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let (generation, metageneration) = parse_token(expected)?;
        self.upload(
            path,
            value,
            tags,
            WriteConds {
                if_generation_match: Some(generation),
                if_metageneration_match: Some(metageneration),
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
        self.upload(
            path,
            value,
            tags,
            WriteConds {
                if_generation_match: Some("0".to_string()),
                if_metageneration_match: None,
            },
        )
        .await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        let rb = self.http.delete(self.object_url(path));
        let resp = self.send(rb).await?;
        check_status(resp.status(), "Delete", path)?;
        Ok(())
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        let (generation, metageneration) = parse_token(expected)?;
        let rb = self.http.delete(self.object_url(path)).query(&[
            ("ifGenerationMatch", generation.as_str()),
            ("ifMetagenerationMatch", metageneration.as_str()),
        ]);
        self.send_conditional(rb, "DeleteIf", path).await?;
        Ok(())
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        let prefix = ensure_trailing_slash(dir_path);
        let mut page_token: Option<String> = None;
        let mut keys: Vec<String> = Vec::new();
        loop {
            let mut query: Vec<(&str, String)> =
                vec![("delimiter", "/".to_string()), ("prefix", prefix.clone())];
            if let Some(t) = &page_token {
                query.push(("pageToken", t.clone()));
            }
            let rb = self.http.get(self.objects_url()).query(&query);
            let resp = self.send(rb).await?;
            check_status(resp.status(), "List", &prefix)?;
            let page: ListResponse = parse_json(resp, "List", &prefix).await?;
            keys.extend(page.prefixes);
            keys.extend(page.items.into_iter().filter_map(|o| o.name));
            match page.next_page_token {
                Some(t) if !t.is_empty() => page_token = Some(t),
                _ => break,
            }
        }
        keys.sort();
        Ok(keys)
    }
}

/// A subset of the GCS object resource JSON.
#[derive(Debug, Deserialize)]
struct ObjectResource {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    generation: Option<String>,
    #[serde(default)]
    metageneration: Option<String>,
    #[serde(default)]
    metadata: Option<HashMap<String, String>>,
}

impl ObjectResource {
    fn version(&self) -> Version {
        let g = self.generation.as_deref().unwrap_or("");
        let m = self.metageneration.as_deref().unwrap_or("");
        Version::new(format!("{g}/{m}"))
    }

    fn tags(&self) -> Tags {
        match &self.metadata {
            Some(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            None => Tags::new(),
        }
    }

    fn metadata(&self) -> Metadata {
        Metadata {
            tags: Arc::new(self.tags()),
            version: self.version(),
        }
    }
}

/// A subset of the GCS object-listing JSON.
#[derive(Debug, Deserialize)]
struct ListResponse {
    #[serde(default)]
    items: Vec<ObjectResource>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// Splits an opaque [`Version`] token back into its `generation` and
/// `metageneration` components. A malformed or null token cannot match any
/// stored object, so it is reported as a failed precondition.
fn parse_token(v: &Version) -> Result<(String, String), BackendError> {
    match v.token.split_once('/') {
        Some((g, m)) if !g.is_empty() && !m.is_empty() => Ok((g.to_string(), m.to_string())),
        _ => Err(BackendError::Precondition),
    }
}

/// A structured diagnostic for a GCS request that returned an unsuccessful HTTP
/// status without mapping to a dedicated [`BackendError`] classification.
#[derive(Debug, thiserror::Error)]
#[error("{op}({path}): gcs status {status}")]
struct GcsStatusError {
    op: &'static str,
    path: String,
    status: u16,
}

impl GcsStatusError {
    fn new(op: &'static str, path: &str, status: StatusCode) -> Self {
        GcsStatusError {
            op,
            path: path.to_string(),
            status: status.as_u16(),
        }
    }

    /// Flattens the structured status error into the shared catch-all, keeping
    /// the typed error inspectable via [`std::error::Error::source`].
    fn into_backend_error(self) -> BackendError {
        // An inherent method rather than a `From` impl on purpose: adding
        // another `From<_> for BackendError` would make `?`/`Ok(())` inference
        // ambiguous at call sites that rely on `BackendError` being the only
        // inferable error type.
        BackendError::Other {
            msg: self.to_string(),
            source: Some(Cause::new(self)),
        }
    }
}

/// Maps a GCS HTTP status onto a [`BackendError`].
///
/// Used only for idempotent requests (reads, `get_metadata`, unconditional
/// write/delete, list); conditional writes use [`check_conditional_status`].
/// A `5xx` on an idempotent request is a transient outage that is always safe
/// to retry (ADR-009), so it surfaces as `Unavailable` rather than a generic
/// `Other`.
fn check_status(status: StatusCode, op: &'static str, path: &str) -> Result<(), BackendError> {
    if status.is_success() {
        return Ok(());
    }
    match status {
        StatusCode::NOT_FOUND => Err(BackendError::NotFound),
        StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => Err(BackendError::Precondition),
        s if s.is_server_error() => Err(BackendError::Unavailable(format!(
            "{op}({path}): transient server error (gcs status {})",
            s.as_u16()
        ))),
        s => Err(GcsStatusError::new(op, path, s).into_backend_error()),
    }
}

/// Maps a non-success status from a *conditional* request (ADR-009). A `412`/
/// `409` is a genuine precondition (the atomic write did not take effect); a
/// `5xx` leaves the write in doubt, since GCS may have applied it before
/// failing, so it is reported as `Unavailable`.
fn check_conditional_status(status: StatusCode, op: &'static str, path: &str) -> BackendError {
    match status {
        StatusCode::NOT_FOUND => BackendError::NotFound,
        StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => BackendError::Precondition,
        s if s.is_server_error() => BackendError::Unavailable(format!(
            "{op}({path}): conditional write outcome unknown (gcs status {})",
            s.as_u16()
        )),
        s => GcsStatusError::new(op, path, s).into_backend_error(),
    }
}

/// Deserializes a JSON response body, annotating failures.
async fn parse_json<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
    op: &str,
    path: &str,
) -> Result<T, BackendError> {
    resp.json::<T>()
        .await
        .map_err(|e| BackendError::with_source(format!("{op}({path}): decoding response"), e))
}

/// Builds the JSON metadata part of a multipart upload.
fn object_metadata_json(name: &str, tags: &Tags) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "name".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    if !tags.is_empty() {
        obj.insert("metadata".to_string(), tags_to_json(tags));
    }
    serde_json::Value::Object(obj).to_string()
}

/// Builds the JSON body of a metadata-only patch.
fn metadata_patch_json(tags: &Tags) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("metadata".to_string(), tags_to_json(tags));
    serde_json::Value::Object(obj).to_string()
}

fn tags_to_json(tags: &Tags) -> serde_json::Value {
    serde_json::Value::Object(
        tags.iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
            .collect(),
    )
}

/// Assembles a `multipart/related` upload body from a JSON metadata part and a
/// binary media part.
fn multipart_body(json: &str, value: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(
        format!("--{BOUNDARY}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(json.as_bytes());
    body.extend_from_slice(
        format!("\r\n--{BOUNDARY}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
    );
    body.extend_from_slice(value);
    body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn ensure_trailing_slash(a: &str) -> String {
    if a.ends_with('/') {
        a.to_string()
    } else {
        format!("{a}/")
    }
}
