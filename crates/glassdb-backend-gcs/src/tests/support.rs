//! In-process GCS test server and its JSON-API protocol support.

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use crate::{BOUNDARY, GcsBackend};

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
    /// Number of mutations to apply but answer with `500` (a lost ack).
    lost_ack: Mutex<i64>,
    /// Number of object GETs to answer with `500` (a transient read outage).
    read_fault: Mutex<i64>,
}

/// An owned GCS test server wired to a real loopback HTTP transport.
pub(super) struct FakeGcs {
    base_url: String,
    address: SocketAddr,
    state: Arc<FakeState>,
    shutdown: Option<oneshot::Sender<()>>,
    accept_task: Option<JoinHandle<()>>,
}

impl FakeGcs {
    /// Starts a server and returns after its loopback listener is bound.
    pub(super) async fn start() -> FakeGcs {
        let state = Arc::new(FakeState {
            store: Mutex::new(Store {
                objects: HashMap::new(),
                gen_ctr: 1,
            }),
            lost_ack: Mutex::new(0),
            read_fault: Mutex::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let accept_task = tokio::spawn(serve(listener, state.clone(), shutdown_rx));
        FakeGcs {
            base_url: format!("http://{address}"),
            address,
            state,
            shutdown: Some(shutdown),
            accept_task: Some(accept_task),
        }
    }

    /// Creates a backend connected to this server.
    pub(super) fn backend(&self) -> GcsBackend {
        GcsBackend::with_endpoint("test", self.base_url.clone())
    }

    /// Apply the next `n` mutations but answer them with `500` (a lost ack).
    pub(super) fn set_lost_ack(&self, n: i64) {
        *self.state.lost_ack.lock().unwrap() = n;
    }

    /// Answer the next `n` object metadata GETs with `500` (a transient read
    /// outage), without touching the stored object.
    pub(super) fn set_read_fault(&self, n: i64) {
        *self.state.read_fault.lock().unwrap() = n;
    }

    /// Stops the server and waits until its listener and connections are gone.
    pub(super) async fn shutdown(mut self) {
        self.signal_shutdown();
        if let Some(task) = self.accept_task.as_mut() {
            task.await.expect("fake GCS server task panicked");
            self.accept_task.take();
        }
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for FakeGcs {
    fn drop(&mut self) {
        self.signal_shutdown();
        if let Some(task) = self.accept_task.take() {
            // Drop cannot await, so cancellation is the only way to ensure an
            // abandoned test server cannot keep its listener or connections.
            task.abort();
        }
    }
}

async fn serve(listener: TcpListener, state: Arc<FakeState>, mut shutdown: oneshot::Receiver<()>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    continue;
                };
                let io = TokioIo::new(stream);
                let state = state.clone();
                connections.spawn(async move {
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
        }
    }
    connections.shutdown().await;
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
        && existing.map(|o| o.metageneration) != Some(m)
    {
        return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
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
    let body = resource_json(&name, &obj);
    drop(store);

    // Lost-ack injection: the insert above is durable, but the client is told the
    // request failed (500), so it cannot know the write landed.
    {
        let mut la = state.lost_ack.lock().unwrap();
        if *la > 0 {
            *la -= 1;
            return error_json(StatusCode::INTERNAL_SERVER_ERROR, "backendError");
        }
    }

    json_response(StatusCode::OK, body)
}

fn get_attrs(state: &FakeState, name: &str) -> Response<Full<Bytes>> {
    {
        // Transient read-outage injection: the object is untouched, but the GET
        // is answered with a 500, modelling a recoverable backend blip.
        let mut rf = state.read_fault.lock().unwrap();
        if *rf > 0 {
            *rf -= 1;
            return error_json(StatusCode::INTERNAL_SERVER_ERROR, "backendError");
        }
    }
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
        && g != o.generation
    {
        return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
    }
    // Conditional read: an unchanged generation answers 304 with no body.
    if let Some(g) = query
        .get("ifGenerationNotMatch")
        .and_then(|v| v.parse::<i64>().ok())
        && g == o.generation
    {
        return Response::builder()
            .status(StatusCode::NOT_MODIFIED)
            .body(Full::new(Bytes::new()))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/octet-stream")
        .header("x-goog-generation", o.generation.to_string())
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
            && g != o.generation
        {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
        if let Some(m) = query
            .get("ifMetagenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
            && m != o.metageneration
        {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
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
            && g != o.generation
        {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
        if let Some(m) = query
            .get("ifMetagenerationMatch")
            .and_then(|v| v.parse::<i64>().ok())
            && m != o.metageneration
        {
            return error_json(StatusCode::PRECONDITION_FAILED, "conditionNotMet");
        }
    }
    store.objects.remove(name);
    {
        let mut la = state.lost_ack.lock().unwrap();
        if *la > 0 {
            *la -= 1;
            return error_json(StatusCode::INTERNAL_SERVER_ERROR, "backendError");
        }
    }
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn list(state: &FakeState, query: &HashMap<String, String>) -> Response<Full<Bytes>> {
    let store = state.store.lock().unwrap();
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let max_results = query
        .get("maxResults")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1000);
    let after = match query.get("pageToken") {
        Some(token) => match decode_page_token(&prefix, token) {
            Some(after) => Some(after),
            None => return error_json(StatusCode::BAD_REQUEST, "invalidPageToken"),
        },
        None => None,
    };

    let mut items: Vec<&String> = Vec::new();
    for k in store.objects.keys() {
        if !k.starts_with(&prefix) {
            continue;
        }
        if after.is_some_and(|after| k.as_str() <= after) {
            continue;
        }
        items.push(k);
    }
    items.sort();
    let truncated = items.len() > max_results;
    items.truncate(max_results);
    let next = truncated
        .then(|| items.last())
        .flatten()
        .map(|last| encode_page_token(&prefix, last));

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
        "nextPageToken": next,
    })
    .to_string();
    json_response(StatusCode::OK, body)
}

fn encode_page_token(prefix: &str, last: &str) -> String {
    format!("{}:{prefix}{last}", prefix.len())
}

fn decode_page_token<'a>(prefix: &str, token: &'a str) -> Option<&'a str> {
    let (prefix_len, body) = token.split_once(':')?;
    let prefix_len = prefix_len.parse::<usize>().ok()?;
    let stored_prefix = body.get(..prefix_len)?;
    let last = body.get(prefix_len..)?;
    (stored_prefix == prefix && last.starts_with(prefix)).then_some(last)
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

#[cfg(test)]
mod lifecycle_tests {
    use std::io::ErrorKind;
    use std::sync::{Arc, Weak};
    use std::time::Duration;

    use glassdb_backend::{Backend, BackendError};
    use tokio::net::TcpListener;

    use super::{FakeGcs, FakeState};

    async fn wait_for_release(address: std::net::SocketAddr, state: Weak<FakeState>) {
        tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                if state.upgrade().is_some() {
                    tokio::task::yield_now().await;
                    continue;
                }
                match TcpListener::bind(address).await {
                    Ok(listener) => {
                        drop(listener);
                        return;
                    }
                    Err(error) if error.kind() == ErrorKind::AddrInUse => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("failed to rebind fake GCS listener: {error}"),
                }
            }
        })
        .await
        .expect("fake GCS listener or tasks outlived their owner");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_start_stop_releases_listener_and_tasks() {
        for explicit_shutdown in [true, false] {
            for _ in 0..3 {
                let fake = FakeGcs::start().await;
                let address = fake.address;
                let state = Arc::downgrade(&fake.state);
                let backend = fake.backend();
                assert!(matches!(
                    backend.read("lifecycle-probe").await,
                    Err(BackendError::NotFound)
                ));
                drop(backend);

                if explicit_shutdown {
                    fake.shutdown().await;
                } else {
                    drop(fake);
                }
                wait_for_release(address, state).await;
            }
        }
    }
}
