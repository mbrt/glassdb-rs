//! Google Cloud Storage backend for GlassDB (ADR-016, ADR-023, ADR-042).
//!
//! Each logical key maps to a single GCS object whose body holds the value.
//! GCS provides native content compare-and-swap through object `generation`
//! preconditions, so the opaque [`Version`] token is the object generation.
//! Conditional reads use `ifGenerationNotMatch`; writes and deletion require an
//! exact generation condition.

use std::sync::Arc;

use async_trait::async_trait;
use glassdb_backend::{
    Backend, BackendError, Cause, ListCursor, ListLimit, ListPage, ReadReply, Version,
};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, RequestBuilder, StatusCode};

const MAX_LIST_PAGE_SIZE: usize = 1_000;
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
    /// Only idempotent reads and listings go through `send`; conditional
    /// mutations use [`Self::send_conditional`]. A transport failure on an
    /// idempotent request is therefore always safe to retry, so it is reported
    /// as `Unavailable` rather than a generic `Other` (ADR-009), letting the
    /// engine recover a transient outage in place.
    async fn send(&self, rb: RequestBuilder) -> Result<reqwest::Response, BackendError> {
        let rb = self.authorize(rb).await?;
        rb.send()
            .await
            .map_err(|e| BackendError::Unavailable(format!("gcs request transport failure: {e}")))
    }

    /// Sends a *conditional* request and maps its outcome with the in-doubt
    /// contract (ADR-009). GCS applies conditional mutations atomically and this
    /// backend does not retry them, so a clean `412`/`409` means the mutation did
    /// not take effect (a genuine `Precondition`). But a transport error or a
    /// `5xx` leaves the outcome unknown — the mutation may have landed before the
    /// failure — so it is reported as `Unavailable` rather than a confident
    /// error or a generic `Other`. An authentication failure happens before the
    /// request is sent, so it is not in-doubt.
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
        check_status(resp.status(), "Read", path)?;
        parse_json(resp, "Read", path).await
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
            });
        }
        Err(BackendError::other(format!(
            "Read({path}): too many concurrent writes during read"
        )))
    }

    /// Uploads `value` as a multipart insert with the required generation
    /// precondition, returning the new version.
    async fn upload(
        &self,
        path: &str,
        value: Vec<u8>,
        if_generation_match: String,
    ) -> Result<Version, BackendError> {
        let body = multipart_body(&object_metadata_json(path), &value);
        let query = [
            ("uploadType", "multipart".to_string()),
            ("ifGenerationMatch", if_generation_match),
        ];
        let rb = self
            .http
            .post(self.upload_url())
            .query(&query)
            .header(
                CONTENT_TYPE,
                format!("multipart/related; boundary={BOUNDARY}"),
            )
            .body(body);
        let resp = self.send_conditional(rb, "Write", path).await?;
        let obj: ObjectResource = parse_json(resp, "Write", path).await.map_err(|error| {
            BackendError::Unavailable(format!(
                "Write({path}): mutation applied but response could not be decoded: {error}"
            ))
        })?;
        obj.generation
            .filter(|generation| !generation.is_empty())
            .map(Version::new)
            .ok_or_else(|| {
                BackendError::Unavailable(format!(
                    "Write({path}): mutation applied but response omitted its generation"
                ))
            })
    }
}

#[async_trait]
impl Backend for GcsBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let attrs = self.attrs(path).await?;
        self.read_from_attrs(path, attrs).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        if expected.is_unset() {
            return self.read(path).await;
        }
        // A single conditional media GET: the body transfers only when the
        // generation differs; an unchanged object answers `304 Not Modified`.
        let rb = self.http.get(self.object_url(path)).query(&[
            ("alt", "media"),
            ("ifGenerationNotMatch", expected.token.as_ref()),
        ]);
        let resp = self.send(rb).await?;
        let status = resp.status();
        if status == StatusCode::NOT_MODIFIED {
            return Err(BackendError::Precondition);
        }
        check_status(status, "ReadIfModified", path)?;
        let version = generation_from_headers(&resp);
        let contents = resp.bytes().await.map_err(|e| {
            BackendError::with_source(format!("ReadIfModified({path}): reading body"), e)
        })?;
        Ok(ReadReply {
            contents: contents.to_vec(),
            version,
        })
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        let generation = parse_token(expected)?;
        self.upload(path, value, generation).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.upload(path, value, "0".to_string()).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        let generation = parse_token(expected)?;
        let rb = self
            .http
            .delete(self.object_url(path))
            .query(&[("ifGenerationMatch", generation)]);
        self.send_conditional(rb, "DeleteIf", path).await?;
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        validate_list_prefix(prefix)?;
        let max_results = u32::try_from(limit.get().min(MAX_LIST_PAGE_SIZE)).unwrap();
        let mut query = vec![
            ("prefix", prefix.to_string()),
            ("maxResults", max_results.to_string()),
        ];
        if let Some(cursor) = cursor {
            query.push(("pageToken", cursor.as_str().to_string()));
        }
        let rb = self.http.get(self.objects_url()).query(&query);
        let resp = self.send(rb).await?;
        if cursor.is_some() && resp.status() == StatusCode::BAD_REQUEST {
            return Err(BackendError::InvalidCursor);
        }
        check_status(resp.status(), "List", prefix)?;
        let page: ListResponse = parse_json(resp, "List", prefix).await?;
        let objects = page.items.into_iter().filter_map(|o| o.name).collect();
        let next = page
            .next_page_token
            .filter(|token| !token.is_empty())
            .map(ListCursor::new);
        Ok(ListPage { objects, next })
    }
}

/// A subset of the GCS object resource JSON.
#[derive(Debug, Deserialize)]
struct ObjectResource {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    generation: Option<String>,
}

impl ObjectResource {
    fn version(&self) -> Version {
        match &self.generation {
            Some(g) => Version::new(g.as_str()),
            None => Version::default(),
        }
    }
}

/// Builds a [`Version`] from the `x-goog-generation` header of a media
/// download. GCS always sets it on a successful object GET; absent it, an unset
/// version is returned (the cache will simply re-read fully next time).
fn generation_from_headers(resp: &reqwest::Response) -> Version {
    resp.headers()
        .get("x-goog-generation")
        .and_then(|v| v.to_str().ok())
        .map(Version::new)
        .unwrap_or_default()
}

/// A subset of the GCS object-listing JSON.
#[derive(Debug, Deserialize)]
struct ListResponse {
    #[serde(default)]
    items: Vec<ObjectResource>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: Option<String>,
}

/// Returns the `generation` carried by an opaque [`Version`] token. A null
/// token cannot match any stored object, so it is reported as a failed
/// precondition.
fn parse_token(v: &Version) -> Result<String, BackendError> {
    if v.token.is_empty() {
        return Err(BackendError::Precondition);
    }
    Ok(v.token.to_string())
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
/// Used only for idempotent reads and listings; conditional mutations use
/// [`check_conditional_status`].
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
/// `409` is a genuine precondition (the atomic mutation did not take effect); a
/// `5xx` leaves the mutation in doubt, since GCS may have applied it before
/// failing, so it is reported as `Unavailable`.
fn check_conditional_status(status: StatusCode, op: &'static str, path: &str) -> BackendError {
    match status {
        StatusCode::NOT_FOUND => BackendError::NotFound,
        StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => BackendError::Precondition,
        s if s.is_server_error() => BackendError::Unavailable(format!(
            "{op}({path}): conditional mutation outcome unknown (gcs status {})",
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

/// Builds the JSON metadata part of a multipart upload (just the object name).
fn object_metadata_json(name: &str) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "name".to_string(),
        serde_json::Value::String(name.to_string()),
    );
    serde_json::Value::Object(obj).to_string()
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

fn validate_list_prefix(prefix: &str) -> Result<(), BackendError> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Ok(())
    } else {
        Err(BackendError::other(format!(
            "list prefix must be empty or end in '/': {prefix:?}"
        )))
    }
}
