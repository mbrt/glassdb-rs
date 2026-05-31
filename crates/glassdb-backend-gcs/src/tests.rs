//! Behavioral tests for the GCS backend, run against a pure-Rust in-process
//! fake implementing the JSON-API subset the backend uses (the analog of the
//! Go tests' `fake-gcs-server`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use glassdb_backend::{
    encode_writer_tag, Backend, BackendError, Tags, Version, WriterId, LAST_WRITER_TAG,
};
use glassdb_concurr::Ctx;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::{GcsBackend, BOUNDARY};

fn ctx() -> Ctx {
    Ctx::background()
}

// ---------------------------------------------------------------------------
// In-process fake GCS server
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct GcsObject {
    bytes: Vec<u8>,
    metadata: BTreeMap<String, String>,
    generation: i64,
    metageneration: i64,
}

struct Store {
    objects: HashMap<String, GcsObject>,
    gen_ctr: i64,
}

struct FakeState {
    store: Mutex<Store>,
}

struct FakeGcs {
    base_url: String,
}

impl FakeGcs {
    async fn start() -> FakeGcs {
        let state = Arc::new(FakeState {
            store: Mutex::new(Store {
                objects: HashMap::new(),
                gen_ctr: 1,
            }),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let io = TokioIo::new(stream);
                let state = state.clone();
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let state = state.clone();
                                async move { handle(state, req).await }
                            }),
                        )
                        .await;
                });
            }
        });
        FakeGcs {
            base_url: format!("http://{addr}"),
        }
    }

    fn url(&self) -> String {
        self.base_url.clone()
    }
}

async fn handle(
    state: Arc<FakeState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let query = query_params(parts.uri.query().unwrap_or(""));
    let content_type = parts
        .headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default()
        .to_vec();

    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();

    // /upload/storage/v1/b/{bucket}/o   (multipart insert)
    let resp = if method == Method::POST && segments.len() == 6 && segments[0] == "upload" {
        insert(&state, &content_type, &query, body)
    } else if segments.len() == 5 && segments[..2] == ["storage", "v1"] && segments[4] == "o" {
        // /storage/v1/b/{bucket}/o   (list)
        match method {
            Method::GET => list(&state, &query),
            _ => error_json(StatusCode::METHOD_NOT_ALLOWED, "notAllowed"),
        }
    } else if segments.len() == 6 && segments[..2] == ["storage", "v1"] && segments[4] == "o" {
        // /storage/v1/b/{bucket}/o/{object}
        let name = pct_decode(segments[5]);
        match method {
            Method::GET if query.get("alt").map(String::as_str) == Some("media") => {
                get_media(&state, &name, &query)
            }
            Method::GET => get_attrs(&state, &name),
            Method::PATCH => patch(&state, &name, &query, body),
            Method::DELETE => delete(&state, &name, &query),
            _ => error_json(StatusCode::METHOD_NOT_ALLOWED, "notAllowed"),
        }
    } else {
        error_json(StatusCode::NOT_FOUND, "notFound")
    };
    Ok(resp)
}

fn insert(
    state: &FakeState,
    content_type: &str,
    query: &HashMap<String, String>,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let boundary = boundary_of(content_type);
    let (name, metadata, media) = parse_multipart(&body, &boundary);

    let mut store = state.store.lock().unwrap();
    let existing = store.objects.get(&name);
    if let Some(g) = query
        .get("ifGenerationMatch")
        .and_then(|v| v.parse::<i64>().ok())
    {
        let ok = if g == 0 {
            existing.is_none()
        } else {
            existing.map(|o| o.generation) == Some(g)
        };
        if !ok {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
    }
    if let Some(m) = query
        .get("ifMetagenerationMatch")
        .and_then(|v| v.parse::<i64>().ok())
    {
        if existing.map(|o| o.metageneration) != Some(m) {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
    }

    store.gen_ctr += 1;
    let generation = store.gen_ctr;
    let obj = GcsObject {
        bytes: media,
        metadata,
        generation,
        metageneration: 1,
    };
    store.objects.insert(name.clone(), obj.clone());
    json_response(StatusCode::OK, resource_json(&name, &obj))
}

fn get_attrs(state: &FakeState, name: &str) -> Response<Full<Bytes>> {
    let store = state.store.lock().unwrap();
    match store.objects.get(name) {
        Some(o) => json_response(StatusCode::OK, resource_json(name, o)),
        None => error_json(StatusCode::NOT_FOUND, "notFound"),
    }
}

fn get_media(
    state: &FakeState,
    name: &str,
    query: &HashMap<String, String>,
) -> Response<Full<Bytes>> {
    let store = state.store.lock().unwrap();
    let Some(o) = store.objects.get(name) else {
        return error_json(StatusCode::NOT_FOUND, "notFound");
    };
    if let Some(g) = query
        .get("ifGenerationMatch")
        .and_then(|v| v.parse::<i64>().ok())
    {
        if g != o.generation {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .body(Full::new(Bytes::from(o.bytes.clone())))
        .unwrap()
}

fn patch(
    state: &FakeState,
    name: &str,
    query: &HashMap<String, String>,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let mut store = state.store.lock().unwrap();
    {
        let Some(o) = store.objects.get(name) else {
            return error_json(StatusCode::NOT_FOUND, "notFound");
        };
        if let Some(g) = query
            .get("ifGenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
        {
            if g != o.generation {
                return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
            }
        }
        if let Some(m) = query
            .get("ifMetagenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
        {
            if m != o.metageneration {
                return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
            }
        }
    }
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
    let metadata = json_metadata(&parsed);
    let o = store.objects.get_mut(name).unwrap();
    o.metadata = metadata;
    o.metageneration += 1;
    json_response(StatusCode::OK, resource_json(name, o))
}

fn delete(state: &FakeState, name: &str, query: &HashMap<String, String>) -> Response<Full<Bytes>> {
    let mut store = state.store.lock().unwrap();
    {
        let Some(o) = store.objects.get(name) else {
            return error_json(StatusCode::NOT_FOUND, "notFound");
        };
        if let Some(g) = query
            .get("ifGenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
        {
            if g != o.generation {
                return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
            }
        }
        if let Some(m) = query
            .get("ifMetagenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
        {
            if m != o.metageneration {
                return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
            }
        }
    }
    store.objects.remove(name);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn list(state: &FakeState, query: &HashMap<String, String>) -> Response<Full<Bytes>> {
    let store = state.store.lock().unwrap();
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delim = query.get("delimiter").cloned().unwrap_or_default();

    let mut items: Vec<&String> = Vec::new();
    let mut prefixes: BTreeSet<String> = BTreeSet::new();
    for k in store.objects.keys() {
        let Some(rest) = k.strip_prefix(&prefix) else {
            continue;
        };
        if !delim.is_empty() {
            if let Some(idx) = rest.find(&delim) {
                prefixes.insert(format!("{prefix}{}", &rest[..=idx]));
                continue;
            }
        }
        items.push(k);
    }
    items.sort();

    let items_json: Vec<serde_json::Value> = items
        .iter()
        .map(|name| {
            let o = &store.objects[*name];
            serde_json::json!({
                "name": name,
                "generation": o.generation.to_string(),
                "metageneration": o.metageneration.to_string(),
            })
        })
        .collect();
    let body = serde_json::json!({
        "kind": "storage#objects",
        "items": items_json,
        "prefixes": prefixes.into_iter().collect::<Vec<_>>(),
    })
    .to_string();
    json_response(StatusCode::OK, body)
}

fn resource_json(name: &str, o: &GcsObject) -> String {
    serde_json::json!({
        "kind": "storage#object",
        "bucket": "test",
        "name": name,
        "generation": o.generation.to_string(),
        "metageneration": o.metageneration.to_string(),
        "size": o.bytes.len().to_string(),
        "metadata": o.metadata,
    })
    .to_string()
}

fn json_metadata(v: &serde_json::Value) -> BTreeMap<String, String> {
    v.get("metadata")
        .and_then(|m| m.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn json_response(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn error_json(status: StatusCode, reason: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "error": {
            "code": status.as_u16(),
            "message": reason,
            "errors": [{ "reason": reason }],
        }
    })
    .to_string();
    json_response(status, body)
}

fn boundary_of(content_type: &str) -> String {
    content_type
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("boundary="))
        .map(|b| b.trim_matches('"').to_string())
        .next()
        .unwrap_or_else(|| BOUNDARY.to_string())
}

/// Parses a `multipart/related` body into the object name, custom metadata, and
/// media bytes.
fn parse_multipart(body: &[u8], boundary: &str) -> (String, BTreeMap<String, String>, Vec<u8>) {
    let sep = format!("--{boundary}");
    let mut json_part: Vec<u8> = Vec::new();
    let mut media_part: Vec<u8> = Vec::new();
    for raw in split_on(body, sep.as_bytes()) {
        let part = trim_leading_crlf(raw);
        if part.starts_with(b"--") || part.is_empty() {
            continue;
        }
        let Some(idx) = find(part, b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&part[..idx]).to_ascii_lowercase();
        let content = trim_trailing_crlf(&part[idx + 4..]);
        if headers.contains("application/json") {
            json_part = content.to_vec();
        } else {
            media_part = content.to_vec();
        }
    }
    let parsed: serde_json::Value = serde_json::from_slice(&json_part).unwrap_or_default();
    let name = parsed
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    (name, json_metadata(&parsed), media_part)
}

fn split_on<'a>(hay: &'a [u8], needle: &[u8]) -> Vec<&'a [u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i + needle.len() <= hay.len() {
        if &hay[i..i + needle.len()] == needle {
            out.push(&hay[start..i]);
            i += needle.len();
            start = i;
        } else {
            i += 1;
        }
    }
    out.push(&hay[start..]);
    out
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

fn trim_leading_crlf(b: &[u8]) -> &[u8] {
    b.strip_prefix(b"\r\n").unwrap_or(b)
}

fn trim_trailing_crlf(b: &[u8]) -> &[u8] {
    b.strip_suffix(b"\r\n").unwrap_or(b)
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
        match b[i] {
            b'%' if i + 2 < b.len() => {
                if let Ok(h) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(h);
                    i += 3;
                    continue;
                }
                out.push(b[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn backend(fake: &FakeGcs) -> GcsBackend {
    GcsBackend::with_endpoint("test", fake.url())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_write_roundtrip() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    for (name, value) in [
        ("non-empty", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("binary", vec![0x00, 0x01, 0x02, 0xff]),
    ] {
        let mut tags = Tags::new();
        tags.insert("key".to_string(), "val".to_string());
        let meta = b.write(&ctx(), name, value.clone(), tags).await.unwrap();
        assert!(!meta.version.is_null());

        let r = b.read(&ctx(), name).await.unwrap();
        assert_eq!(r.contents, value, "case {name}");
        assert_eq!(r.tags.get("key").map(String::as_str), Some("val"));
        assert_eq!(r.version, meta.version);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_produces_fresh_version_each_time() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let m1 = b
        .write(&ctx(), "k", b"same".to_vec(), Tags::new())
        .await
        .unwrap();
    let m2 = b
        .write(&ctx(), "k", b"same".to_vec(), Tags::new())
        .await
        .unwrap();
    assert_ne!(m1.version, m2.version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_merges_and_cas() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);

    let writer = WriterId::new(b"tx-1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    tags.insert("lock-type".to_string(), "-".to_string());
    let m0 = b.write(&ctx(), "k", b"value".to_vec(), tags).await.unwrap();

    let mut new_tags = Tags::new();
    new_tags.insert("lock-type".to_string(), "w".to_string());
    new_tags.insert("locked-by".to_string(), "tx2".to_string());
    let m1 = b
        .set_tags_if(&ctx(), "k", &m0.version, new_tags)
        .await
        .unwrap();
    assert_ne!(m0.version, m1.version);
    // The last-writer tag is preserved across a lock-only update.
    assert_eq!(
        m1.tags.get(LAST_WRITER_TAG).map(String::as_str),
        Some(encode_writer_tag(&writer).as_str())
    );
    assert_eq!(m1.tags.get("lock-type").map(String::as_str), Some("w"));
    assert_eq!(m1.tags.get("locked-by").map(String::as_str), Some("tx2"));

    // The object body is untouched by a tag update.
    let r = b.read(&ctx(), "k").await.unwrap();
    assert_eq!(r.contents, b"value");

    // The now-stale version fails the precondition.
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if(&ctx(), "k", &m0.version, t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_not_found() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if(&ctx(), "missing", &Version::new("1/1"), t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    b.write_if_not_exists(&ctx(), "k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();
    let err = b
        .write_if_not_exists(&ctx(), "k", b"b".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let r = b.read(&ctx(), "k").await.unwrap();
    assert_eq!(r.contents, b"a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_cas() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let m0 = b
        .write(&ctx(), "k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();

    let err = b
        .write_if(
            &ctx(),
            "k",
            b"b".to_vec(),
            &Version::new("999/999"),
            Tags::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let m1 = b
        .write_if(&ctx(), "k", b"b".to_vec(), &m0.version, Tags::new())
        .await
        .unwrap();
    assert_ne!(m0.version, m1.version);
    let r = b.read(&ctx(), "k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_null_version_fails_precondition() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let m0 = b
        .write(&ctx(), "k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();

    let err = b
        .write_if(&ctx(), "k", b"b".to_vec(), &Version::default(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b.read(&ctx(), "k").await.unwrap();
    assert_eq!(r.contents, b"a");
    assert_eq!(r.version, m0.version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let writer = WriterId::new(b"w1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    b.write(&ctx(), "k", b"x".to_vec(), tags).await.unwrap();

    let err = b.read_if_modified(&ctx(), "k", &writer).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b
        .read_if_modified(&ctx(), "k", &WriterId::new(b"other".to_vec()))
        .await
        .unwrap();
    assert_eq!(r.contents, b"x");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let m0 = b
        .write(&ctx(), "k", b"x".to_vec(), Tags::new())
        .await
        .unwrap();

    let err = b
        .delete_if(&ctx(), "k", &Version::new("999/999"))
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    b.read(&ctx(), "k").await.unwrap();

    b.delete_if(&ctx(), "k", &m0.version).await.unwrap();
    let err = b.read(&ctx(), "k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_unconditional() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    b.write(&ctx(), "k", b"x".to_vec(), Tags::new())
        .await
        .unwrap();
    b.delete(&ctx(), "k").await.unwrap();
    let err = b.read(&ctx(), "k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_and_metadata_not_found() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    let err = b.read(&ctx(), "missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
    let err = b.get_metadata(&ctx(), "missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_with_subdirs() {
    let fake = FakeGcs::start().await;
    let b = backend(&fake);
    for name in ["d/a/1", "d/a/2", "d/a/b/1", "d/c/1", "d/root"] {
        b.write(&ctx(), name, name.as_bytes().to_vec(), Tags::new())
            .await
            .unwrap();
    }
    let got = b.list(&ctx(), "d").await.unwrap();
    assert_eq!(got, vec!["d/a/", "d/c/", "d/root"]);
    let got = b.list(&ctx(), "d/a").await.unwrap();
    assert_eq!(got, vec!["d/a/1", "d/a/2", "d/a/b/"]);
}
