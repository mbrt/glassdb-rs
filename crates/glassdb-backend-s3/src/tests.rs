//! Behavioral tests for the S3 backend, run against a pure-Rust in-process
//! fake S3 server (the analog of the Go tests' `gofakes3` + `httptest.Server`).

use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use bytes::Bytes;
use glassdb_backend::{
    Backend, BackendError, LAST_WRITER_TAG, Tags, Version, WriterId, encode_writer_tag,
};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::{Builder, S3Backend};

// ---------------------------------------------------------------------------
// In-process fake S3 server
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct StoredObject {
    body: Vec<u8>,
    meta: HashMap<String, String>,
    etag: String,
}

#[derive(Default)]
struct SlowDown {
    remaining: i64,
    method: Option<Method>,
}

/// Models a lost acknowledgement: the next `remaining` PUTs are *applied*
/// normally but then answered with a `500 InternalError` instead of success, so
/// the client cannot tell whether the write landed.
#[derive(Default)]
struct LostAck {
    remaining: i64,
}

struct FakeState {
    objects: Mutex<HashMap<String, StoredObject>>,
    etag_ctr: Mutex<u64>,
    slow: Mutex<SlowDown>,
    lost_ack: Mutex<LostAck>,
}

/// A minimal in-process S3 server implementing just the REST subset the backend
/// uses, with optional `503 SlowDown` injection to drive the retry tests.
struct FakeS3 {
    base_url: String,
    state: Arc<FakeState>,
}

impl FakeS3 {
    async fn start() -> FakeS3 {
        let state = Arc::new(FakeState {
            objects: Mutex::new(HashMap::new()),
            etag_ctr: Mutex::new(1),
            slow: Mutex::new(SlowDown::default()),
            lost_ack: Mutex::new(LostAck::default()),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let io = TokioIo::new(stream);
                let st = st.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let st = st.clone();
                                async move { handle(st, req).await }
                            }),
                        )
                        .await;
                });
            }
        });
        FakeS3 {
            base_url: format!("http://{addr}"),
            state,
        }
    }

    fn url(&self) -> String {
        self.base_url.clone()
    }

    /// Fail the next `n` requests matching `method` (or all when `None`) with a
    /// `503 SlowDown` before serving normally.
    fn set_slowdown(&self, n: i64, method: Option<Method>) {
        let mut s = self.state.slow.lock().unwrap();
        s.remaining = n;
        s.method = method;
    }

    fn slowdown_remaining(&self) -> i64 {
        self.state.slow.lock().unwrap().remaining
    }

    /// Apply the next `n` PUTs but answer them with `500` (a lost ack).
    fn set_lost_ack(&self, n: i64) {
        self.state.lost_ack.lock().unwrap().remaining = n;
    }
}

async fn handle(
    state: Arc<FakeState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // SlowDown injection (port of the Go SlowDownTransport).
    {
        let mut s = state.slow.lock().unwrap();
        let matches = s.remaining > 0 && s.method.as_ref().is_none_or(|m| m == req.method());
        if matches {
            s.remaining -= 1;
            drop(s);
            return Ok(slow_down());
        }
    }

    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();
    let body = body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();

    let trimmed = path.trim_start_matches('/');
    let key = match trimmed.split_once('/') {
        Some((_bucket, k)) => k.to_string(),
        None => String::new(),
    };

    let resp = if key.is_empty() {
        // Bucket-level request.
        if method == Method::GET && query.contains("list-type=2") {
            list_objects(&state, &query)
        } else {
            // CreateBucket and anything else: accept.
            ok_empty()
        }
    } else {
        match method {
            Method::GET => get_object(&state, &key),
            Method::HEAD => head_object(&state, &key),
            Method::PUT => put_object(&state, &key, &parts.headers, body.to_vec()),
            Method::DELETE => delete_object(&state, &key),
            _ => xml_error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "nope"),
        }
    };
    Ok(resp)
}

fn get_object(state: &FakeState, key: &str) -> Response<Full<Bytes>> {
    let objs = state.objects.lock().unwrap();
    match objs.get(key) {
        Some(o) => object_response(o, true),
        None => xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "key not found"),
    }
}

fn head_object(state: &FakeState, key: &str) -> Response<Full<Bytes>> {
    let objs = state.objects.lock().unwrap();
    match objs.get(key) {
        Some(o) => object_response(o, false),
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::new()))
            .unwrap(),
    }
}

fn put_object(
    state: &FakeState,
    key: &str,
    headers: &hyper::HeaderMap,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let if_match = header_str(headers, "if-match");
    let if_none_match = header_str(headers, "if-none-match");

    let mut meta = HashMap::new();
    for (name, val) in headers {
        if let Some(k) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(v) = val.to_str()
        {
            meta.insert(k.to_string(), v.to_string());
        }
    }

    let mut objs = state.objects.lock().unwrap();
    let existing = objs.get(key);
    if let Some(inm) = &if_none_match
        && inm == "*"
        && existing.is_some()
    {
        return xml_error(
            StatusCode::PRECONDITION_FAILED,
            "PreconditionFailed",
            "object exists",
        );
    }
    if let Some(im) = &if_match {
        match existing {
            Some(o) if &o.etag == im => {}
            _ => {
                return xml_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "etag mismatch",
                );
            }
        }
    }

    let etag = {
        let mut ctr = state.etag_ctr.lock().unwrap();
        let e = format!("\"{ctr}\"");
        *ctr += 1;
        e
    };
    objs.insert(
        key.to_string(),
        StoredObject {
            body,
            meta,
            etag: etag.clone(),
        },
    );
    drop(objs);

    // Lost-ack injection: the write above is durable, but the client is told the
    // request failed (500), so it cannot know the write landed.
    {
        let mut la = state.lost_ack.lock().unwrap();
        if la.remaining > 0 {
            la.remaining -= 1;
            drop(la);
            return xml_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "we encountered an internal error",
            );
        }
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("ETag", etag)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn delete_object(state: &FakeState, key: &str) -> Response<Full<Bytes>> {
    state.objects.lock().unwrap().remove(key);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn list_objects(state: &FakeState, query: &str) -> Response<Full<Bytes>> {
    let params = query_params(query);
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delim = params.get("delimiter").cloned().unwrap_or_default();

    let objs = state.objects.lock().unwrap();
    let mut contents: Vec<String> = Vec::new();
    let mut common: BTreeSet<String> = BTreeSet::new();
    for k in objs.keys() {
        let Some(rest) = k.strip_prefix(&prefix) else {
            continue;
        };
        if !delim.is_empty()
            && let Some(idx) = rest.find(&delim)
        {
            common.insert(format!("{prefix}{}", &rest[..=idx]));
            continue;
        }
        contents.push(k.clone());
    }
    contents.sort();

    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );
    xml.push_str(&format!("<Name>test</Name><Prefix>{}</Prefix><MaxKeys>1000</MaxKeys><Delimiter>{}</Delimiter><IsTruncated>false</IsTruncated>", xml_escape(&prefix), xml_escape(&delim)));
    for k in &contents {
        xml.push_str(&format!(
            "<Contents><Key>{}</Key></Contents>",
            xml_escape(k)
        ));
    }
    for p in &common {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(p)
        ));
    }
    xml.push_str("</ListBucketResult>");

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap()
}

fn object_response(o: &StoredObject, with_body: bool) -> Response<Full<Bytes>> {
    let mut b = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", &o.etag);
    for (k, v) in &o.meta {
        b = b.header(format!("x-amz-meta-{k}"), v);
    }
    let body = if with_body {
        Bytes::from(o.body.clone())
    } else {
        b = b.header("content-length", o.body.len().to_string());
        Bytes::new()
    };
    b.body(Full::new(body)).unwrap()
}

fn slow_down() -> Response<Full<Bytes>> {
    xml_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "SlowDown",
        "Please reduce your request rate.",
    )
}

fn xml_error(status: StatusCode, code: &str, msg: &str) -> Response<Full<Bytes>> {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{msg}</Message></Error>"#
    );
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap()
}

fn ok_empty() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn header_str(headers: &hyper::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn query_params(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (pct_decode(k), pct_decode(v))
        })
        .collect()
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(h) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(h);
            i += 3;
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

fn client_for(fake: &FakeS3) -> Client {
    let conf = aws_sdk_s3::config::Builder::default()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "test"))
        .endpoint_url(fake.url())
        .force_path_style(true)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .build();
    Client::from_conf(conf)
}

fn backend(fake: &FakeS3) -> S3Backend {
    S3Backend::new(client_for(fake), "test")
}

fn builder(fake: &FakeS3) -> Builder {
    S3Backend::builder(client_for(fake), "test")
}

/// A standard retryer that retries the same errors as the default (incl. 503
/// SlowDown) but with negligible backoff, keeping the tests quick.
fn fast_retry() -> RetryConfig {
    RetryConfig::standard()
        .with_max_attempts(5)
        .with_initial_backoff(std::time::Duration::from_millis(1))
        .with_max_backoff(std::time::Duration::from_millis(1))
}

// ---------------------------------------------------------------------------
// Tests (ported from backend/s3/s3_test.go)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_strips_nonce() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for (name, value) in [
        ("non-empty", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("binary", vec![0x00, 0x01, 0x02, 0xff]),
    ] {
        let mut tags = Tags::new();
        tags.insert("key".to_string(), "val".to_string());
        let meta = b.write(name, value.clone(), tags).await.unwrap();
        assert!(!meta.version.is_null());

        let r = b.read(name).await.unwrap();
        assert_eq!(r.contents, value, "case {name}");
        assert_eq!(r.tags.get("key").map(String::as_str), Some("val"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_produces_fresh_version_each_time() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    // Re-uploading identical bytes must still change the version because the
    // nonce forces a fresh ETag.
    let m1 = b.write("k", b"same".to_vec(), Tags::new()).await.unwrap();
    let m2 = b.write("k", b"same".to_vec(), Tags::new()).await.unwrap();
    assert_ne!(m1.version, m2.version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_merges_and_cas() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);

    let writer = WriterId::new(b"tx-1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    tags.insert("lock-type".to_string(), "-".to_string());
    let m0 = b.write("k", b"value".to_vec(), tags).await.unwrap();

    let mut new_tags = Tags::new();
    new_tags.insert("lock-type".to_string(), "w".to_string());
    new_tags.insert("locked-by".to_string(), "tx2".to_string());
    let m1 = b.set_tags_if("k", &m0.version, new_tags).await.unwrap();
    assert_ne!(m0.version, m1.version);
    assert_eq!(
        m1.tags.get(LAST_WRITER_TAG).map(String::as_str),
        Some(encode_writer_tag(&writer).as_str())
    );
    assert_eq!(m1.tags.get("lock-type").map(String::as_str), Some("w"));
    assert_eq!(m1.tags.get("locked-by").map(String::as_str), Some("tx2"));

    // The underlying value is untouched by a tag update.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"value");

    // The now-stale version fails the precondition.
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b.set_tags_if("k", &m0.version, t).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if("missing", &Version::new("\"x\""), t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();
    let err = b
        .write_if_not_exists("k", b"b".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_cas() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    let err = b
        .write_if("k", b"b".to_vec(), &Version::new("\"stale\""), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let m1 = b
        .write_if("k", b"b".to_vec(), &m0.version, Tags::new())
        .await
        .unwrap();
    assert_ne!(m0.version, m1.version);
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_null_version_fails_precondition() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    // A null expected version has an empty token; it must fail rather than
    // overwrite unconditionally.
    let err = b
        .write_if("k", b"b".to_vec(), &Version::default(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
    assert_eq!(r.version, m0.version);

    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if("k", &Version::default(), t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let writer = WriterId::new(b"w1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    b.write("k", b"x".to_vec(), tags).await.unwrap();

    let err = b.read_if_modified("k", &writer).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b
        .read_if_modified("k", &WriterId::new(b"other".to_vec()))
        .await
        .unwrap();
    assert_eq!(r.contents, b"x");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"x".to_vec(), Tags::new()).await.unwrap();

    let err = b
        .delete_if("k", &Version::new("\"wrong\""))
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    b.read("k").await.unwrap();

    b.delete_if("k", &m0.version).await.unwrap();
    let err = b.read("k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_and_metadata_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let err = b.read("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
    let err = b.get_metadata("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_with_subdirs() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for name in ["d/a/1", "d/a/2", "d/a/b/1", "d/c/1", "d/root"] {
        b.write(name, name.as_bytes().to_vec(), Tags::new())
            .await
            .unwrap();
    }
    let got = b.list("d").await.unwrap();
    assert_eq!(got, vec!["d/a/", "d/c/", "d/root"]);
    let got = b.list("d/a").await.unwrap();
    assert_eq!(got, vec!["d/a/1", "d/a/2", "d/a/b/"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();
    fake.set_slowdown(2, Some(Method::PUT));

    b.write("k", b"v".to_vec(), Tags::new()).await.unwrap();
    assert_eq!(fake.slowdown_remaining(), 0);

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();

    // The write is a PUT, so it is not throttled here.
    b.write("k", b"v".to_vec(), Tags::new()).await.unwrap();

    fake.set_slowdown(2, Some(Method::GET));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
    assert_eq!(fake.slowdown_remaining(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nop_retryer_surfaces_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).disable_retries().build();
    fake.set_slowdown(1, Some(Method::PUT));

    let err = b.write("k", b"v".to_vec(), Tags::new()).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("SlowDown"), "got: {msg}");
}

// In-doubt contract (ADR-009): a conditional write whose ack is lost must NOT be
// reported as a confident `Precondition`. Object storage has no at-most-once
// request id, so when the SDK (or any layer) re-sends a conditional PUT whose
// first attempt landed, the retry observes a precondition failure for its own
// write that is indistinguishable from a real conflict. The S3 backend therefore
// owns the conditional-write retry loop and surfaces such an outcome as
// `Unavailable`; the engine then fails the transaction in-doubt rather than
// retrying it into a double-apply. These tests would see `Precondition` against
// the pre-fix code (which let the SDK retryer mask the lost ack).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists_lost_ack_is_in_doubt() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);

    // The create lands, but its ack is lost; the re-send sees the object exists
    // and gets 412.
    fake.set_lost_ack(1);
    let err = b
        .write_if_not_exists("k", b"v".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "expected Unavailable (in-doubt), got {err:?}"
    );

    // The first attempt really did persist the object.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_lost_ack_is_in_doubt() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    // The CAS write lands (changing the ETag), but its ack is lost; the re-send's
    // If-Match no longer matches and gets 412.
    fake.set_lost_ack(1);
    let err = b
        .write_if("k", b"b".to_vec(), &m0.version, Tags::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "expected Unavailable (in-doubt), got {err:?}"
    );

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_conflict_still_precondition() {
    // Guard against over-eagerly tainting: a genuine conflict with no lost ack
    // must still be a retryable `Precondition`, not in-doubt.
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();
    let err = b
        .write_if_not_exists("k", b"b".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition), "got {err:?}");
}
