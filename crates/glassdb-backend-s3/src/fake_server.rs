//! A pure-Rust, in-process fake S3 server (the analog of the Go tests'
//! `gofakes3` + `httptest.Server`).
//!
//! It implements just the REST subset the [`crate::S3Backend`] uses and talks
//! plain HTTP/1.1 over a real loopback socket, so a real [`aws_sdk_s3::Client`]
//! exercises its full transport stack (SDK → smithy → hyper connection pool →
//! TCP) against it. That is the key difference from the in-memory
//! `DelayBackend`, which never touches HTTP: this is what lets a benchmark
//! reproduce *client transport* behavior (connection pooling, head-of-line
//! blocking under load) locally, with no AWS account.
//!
//! Two knobs make it useful beyond unit tests (see [`FakeS3Options`]):
//!
//! * **Simulated latency** — each served operation sleeps for a lognormally
//!   distributed time derived from a [`DelayOptions`] profile (e.g.
//!   [`s3_delays`](glassdb_backend::middleware::s3_delays)). Without it a
//!   loopback server answers in microseconds and the connection pool is never
//!   stressed, so the transport effects under study never appear.
//! * **Connection counting** — every accepted TCP connection bumps an optional
//!   shared counter, giving the server-side connection-churn signal the Rust
//!   SDK does not surface on the client side.
//!
//! Fault injection (`503 SlowDown`, lost acknowledgements) is retained for the
//! retry tests.

use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::{
    BehaviorVersion, Credentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use bytes::Bytes;
use glassdb_backend::middleware::{DelayOptions, Latency};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rand_distr::{Distribution, StandardNormal};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Public configuration
// ---------------------------------------------------------------------------

/// Options for [`FakeS3::start_with`].
#[derive(Default)]
pub struct FakeS3Options {
    /// When set, every served operation sleeps for a simulated duration derived
    /// from this profile (e.g. [`s3_delays`](glassdb_backend::middleware::s3_delays)),
    /// honoring its `scale`. `None` serves with no added latency (the default,
    /// used by the unit tests).
    pub latency: Option<DelayOptions>,
    /// When set, every accepted TCP connection increments this counter. Lets a
    /// caller observe server-side connection churn across a measurement window.
    pub conn_counter: Option<Arc<AtomicU64>>,
}

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
    latency: Option<LatencyModel>,
}

/// A minimal in-process S3 server implementing just the REST subset the backend
/// uses, with optional latency and `503 SlowDown` / lost-ack injection.
pub struct FakeS3 {
    base_url: String,
    state: Arc<FakeState>,
}

impl FakeS3 {
    /// Starts a fake with no added latency and no connection counting (the
    /// configuration the unit tests use).
    pub async fn start() -> FakeS3 {
        Self::start_with(FakeS3Options::default()).await
    }

    /// Starts a fake configured by `opts`, returning once it is accepting
    /// connections.
    ///
    /// The server runs on its **own** multi-threaded runtime in a dedicated,
    /// detached thread, so it never competes with the caller's tasks for
    /// scheduling. That isolation matters under load: if `accept` shared a
    /// runtime with hundreds of busy client workers it would be starved when
    /// they all open connections at once, which surfaces on the client as
    /// `dispatch failure` (a connect timeout). The thread runs for the process
    /// lifetime; dropping the returned handle does not stop it.
    pub async fn start_with(opts: FakeS3Options) -> FakeS3 {
        let state = Arc::new(FakeState {
            objects: Mutex::new(HashMap::new()),
            etag_ctr: Mutex::new(1),
            slow: Mutex::new(SlowDown::default()),
            lost_ack: Mutex::new(LostAck::default()),
            latency: opts.latency.map(LatencyModel::from_opts),
        });
        let st = state.clone();
        let conns = opts.conn_counter.clone();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        std::thread::Builder::new()
            .name("fake-s3".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    // A handful of threads drives thousands of (mostly idle,
                    // latency-sleeping) connections; keep it small so the server
                    // does not oversubscribe the box and skew the measurement.
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("build fake-s3 runtime");
                rt.block_on(serve(st, conns, addr_tx));
            })
            .expect("spawn fake-s3 thread");
        let addr = addr_rx.recv().expect("fake-s3 failed to bind");
        FakeS3 {
            base_url: format!("http://{addr}"),
            state,
        }
    }

    /// The base URL to pass to the S3 client's `endpoint_url`.
    pub fn url(&self) -> String {
        self.base_url.clone()
    }

    /// Fail the next `n` requests matching `method` (or all when `None`) with a
    /// `503 SlowDown` before serving normally.
    pub fn set_slowdown(&self, n: i64, method: Option<Method>) {
        let mut s = self.state.slow.lock().unwrap();
        s.remaining = n;
        s.method = method;
    }

    /// How many injected `503 SlowDown` responses are still pending.
    pub fn slowdown_remaining(&self) -> i64 {
        self.state.slow.lock().unwrap().remaining
    }

    /// Apply the next `n` PUTs but answer them with `500` (a lost ack).
    pub fn set_lost_ack(&self, n: i64) {
        self.state.lost_ack.lock().unwrap().remaining = n;
    }

    /// An [`aws_sdk_s3::config::Builder`] pre-wired to talk to this fake: its
    /// loopback `endpoint_url`, dummy static credentials, a placeholder region,
    /// path-style addressing, and checksum validation disabled (the fake rejects
    /// the checksum trailers the SDK would otherwise add). Callers that need to
    /// layer extra config (a custom `http_client`, request interceptors) start
    /// from here and then `.build()`. For the common case use [`FakeS3::client`]
    /// / [`FakeS3::backend`].
    pub fn client_config(&self) -> aws_sdk_s3::config::Builder {
        aws_sdk_s3::config::Builder::default()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .endpoint_url(self.url())
            .force_path_style(true)
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
    }

    /// A ready [`aws_sdk_s3::Client`] wired to this fake with the SDK's default
    /// HTTP connector (see [`FakeS3::client_config`] to customize the transport).
    pub fn client(&self) -> aws_sdk_s3::Client {
        aws_sdk_s3::Client::from_conf(self.client_config().build())
    }

    /// A ready [`S3Backend`](crate::S3Backend) over this fake and `bucket`.
    pub fn backend(&self, bucket: impl Into<String>) -> crate::S3Backend {
        crate::S3Backend::new(self.client(), bucket)
    }
}

/// The accept loop, run on the dedicated server runtime. Binds an ephemeral
/// loopback port, reports it back over `addr_tx`, then serves each connection on
/// its own task.
async fn serve(
    state: Arc<FakeState>,
    conns: Option<Arc<AtomicU64>>,
    addr_tx: std::sync::mpsc::Sender<std::net::SocketAddr>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    addr_tx.send(listener.local_addr().unwrap()).unwrap();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        if let Some(c) = &conns {
            c.fetch_add(1, Ordering::Relaxed);
        }
        let io = TokioIo::new(stream);
        let st = state.clone();
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

    let is_list = key.is_empty() && method == Method::GET && query.contains("list-type=2");

    // Simulate the operation's wire latency before serving, so the client's
    // connection pool sees realistic in-flight times.
    if let Some(m) = &state.latency {
        m.sleep_for(&method, is_list).await;
    }

    let resp = if key.is_empty() {
        // Bucket-level request.
        if is_list {
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
// Simulated latency
// ---------------------------------------------------------------------------

/// Per-operation lognormal latency, derived from a [`DelayOptions`] profile.
/// HTTP methods are mapped to the backend operation they implement so the
/// served latencies match the simulated `DelayBackend`: a `set_tags_if` (a GET
/// then a PUT) naturally costs `obj_read + obj_write`, matching S3's lack of a
/// metadata-only update.
struct LatencyModel {
    get: Lognormal,
    head: Lognormal,
    put: Lognormal,
    delete: Lognormal,
    list: Lognormal,
    scale: f64,
}

impl LatencyModel {
    fn from_opts(o: DelayOptions) -> Self {
        let scale = if o.scale == 0.0 { 1.0 } else { o.scale };
        LatencyModel {
            get: Lognormal::from_latency(o.obj_read),
            head: Lognormal::from_latency(o.meta_read),
            put: Lognormal::from_latency(o.obj_write),
            delete: Lognormal::from_latency(o.obj_write),
            list: Lognormal::from_latency(o.list),
            scale,
        }
    }

    async fn sleep_for(&self, method: &Method, is_list: bool) {
        let ln = if is_list {
            self.list
        } else {
            match *method {
                Method::GET => self.get,
                Method::HEAD => self.head,
                Method::PUT => self.put,
                Method::DELETE => self.delete,
                _ => return,
            }
        };
        let secs = ln.sample_ms() * self.scale / 1_000.0;
        if secs.is_finite() && secs > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
        }
    }
}

/// A lognormal distribution over operation durations, in milliseconds. Mirrors
/// the one in `glassdb_backend`'s `DelayBackend` so the served latencies track
/// the simulated backend.
#[derive(Clone, Copy)]
struct Lognormal {
    mu: f64,
    sigma: f64,
}

impl Lognormal {
    fn from_latency(l: Latency) -> Self {
        let mean = l.mean.as_secs_f64() * 1_000.0;
        let std_dev = l.std_dev.as_secs_f64() * 1_000.0;
        if mean <= 0.0 {
            return Lognormal {
                mu: f64::NEG_INFINITY,
                sigma: 0.0,
            };
        }
        let s_by_m = std_dev / mean;
        let v = (s_by_m * s_by_m + 1.0).ln();
        Lognormal {
            mu: mean.ln() - 0.5 * v,
            sigma: v.sqrt(),
        }
    }

    fn sample_ms(&self) -> f64 {
        let n: f64 = StandardNormal.sample(&mut rand::rng());
        (n * self.sigma + self.mu).exp()
    }
}
