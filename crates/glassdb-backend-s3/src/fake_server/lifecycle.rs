use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_s3::config::{
    AsyncSleep, BehaviorVersion, Credentials, Region, RequestChecksumCalculation,
    ResponseChecksumValidation, Sleep,
};
use aws_smithy_async::time::TimeSource;
use glassdb_concurr::rt;
use hyper::Method;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::TcpSocket;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use super::FakeS3Options;
use super::latency::LatencyModel;
use super::routing::handle;
use super::state::FakeState;

/// Listen backlog for the server socket, well above tokio's default of 1024.
/// High concurrency in aws-bench open connections in bursts, which have issues
/// with the default. A deep backlog absorbs that burst so a momentary accept
/// stall does not drop SYNs, which the client would otherwise see as an
/// intermittent `dispatch failure`. The kernel caps this at
/// `net.core.somaxconn`.
const LISTEN_BACKLOG: u32 = 8192;

/// Drop must not be able to wedge a test process if the server thread fails to
/// observe its shutdown signal. Callers that need failure reporting should use
/// [`FakeS3::shutdown`], which joins without a timeout.
const DROP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// A defensive destructor deadline must keep advancing even when model time is
/// paused; request latency continues to use the simulation-aware runtime seam.
fn drop_deadline_now() -> std::time::Instant {
    std::time::Instant::now()
}

/// A minimal in-process S3 server implementing just the REST subset the backend
/// uses, with optional latency and `503 SlowDown` / lost-ack injection.
pub struct FakeS3 {
    base_url: String,
    state: Arc<FakeState>,
    shutdown: Option<oneshot::Sender<()>>,
    stopped: Mutex<std::sync::mpsc::Receiver<()>>,
    server_thread: Option<std::thread::JoinHandle<()>>,
    #[cfg(test)]
    server_thread_lifetime: std::sync::Weak<()>,
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
    /// The server runs on its **own** multi-threaded runtime in a dedicated
    /// thread, so it never competes with the caller's tasks for
    /// scheduling. That isolation matters under load: if `accept` shared a
    /// runtime with hundreds of busy client workers it would be starved when
    /// they all open connections at once, which surfaces on the client as
    /// `dispatch failure` (a connect timeout). The returned fake owns that
    /// thread; [`FakeS3::shutdown`] or dropping the fake stops it.
    pub async fn start_with(opts: FakeS3Options) -> FakeS3 {
        let state =
            Arc::new(FakeState::new(opts.latency.map(|profile| {
                LatencyModel::from_profile(profile, opts.entropy_seed)
            })));
        let st = state.clone();
        let conns = opts.conn_counter.clone();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let (stopped_tx, stopped) = std::sync::mpsc::channel();
        #[cfg(test)]
        let thread_lifetime = Arc::new(());
        #[cfg(test)]
        let server_thread_lifetime = Arc::downgrade(&thread_lifetime);
        let server_thread = std::thread::Builder::new()
            .name("fake-s3".to_string())
            .spawn(move || {
                #[cfg(test)]
                let _thread_lifetime = thread_lifetime;
                let rt = tokio::runtime::Builder::new_multi_thread()
                    // A handful of threads drives thousands of (mostly idle,
                    // latency-sleeping) connections; keep it small so the server
                    // does not oversubscribe the box and skew the measurement.
                    .worker_threads(4)
                    .enable_all()
                    .build()
                    .expect("build fake-s3 runtime");
                rt.block_on(serve(st, conns, addr_tx, shutdown_rx));
                // Runtime drop joins its worker threads. Report completion only
                // after none of them can outlive the owner.
                drop(rt);
                let _ = stopped_tx.send(());
            })
            .expect("spawn fake-s3 thread");
        let addr = match addr_rx.recv() {
            Ok(addr) => addr,
            Err(_) => {
                let _ = server_thread.join();
                panic!("fake-s3 failed to bind");
            }
        };
        FakeS3 {
            base_url: format!("http://{addr}"),
            state,
            shutdown: Some(shutdown),
            stopped: Mutex::new(stopped),
            server_thread: Some(server_thread),
            #[cfg(test)]
            server_thread_lifetime,
        }
    }

    /// Stops the server and joins its dedicated thread.
    ///
    /// Prefer this over relying on [`Drop`] when a server-thread panic must be
    /// reported to the caller. The drop fallback is deliberately time-bounded.
    pub fn shutdown(mut self) {
        self.signal_shutdown();
        if let Some(server_thread) = self.server_thread.take() {
            server_thread
                .join()
                .expect("fake-s3 server thread panicked");
        }
    }

    /// The base URL to pass to the S3 client's `endpoint_url`.
    pub fn url(&self) -> String {
        self.base_url.clone()
    }

    /// Fail the next `n` requests matching `method` (or all when `None`) with a
    /// `503 SlowDown` before serving normally.
    pub fn set_slowdown(&self, n: i64, method: Option<Method>) {
        self.state.faults.set_slowdown(n, method);
    }

    /// How many injected `503 SlowDown` responses are still pending.
    pub fn slowdown_remaining(&self) -> i64 {
        self.state.faults.slowdown_remaining()
    }

    /// Apply the next `n` mutations but answer them with `500` (a lost ack).
    pub fn set_lost_ack(&self, n: i64) {
        self.state.faults.set_lost_ack(n);
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
            .sleep_impl(ModelTimeSleep)
            .time_source(ModelTimeSource)
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

    #[cfg(test)]
    pub(crate) fn server_thread_lifetime(&self) -> std::sync::Weak<()> {
        self.server_thread_lifetime.clone()
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        self.signal_shutdown();
        let Some(server_thread) = self.server_thread.take() else {
            return;
        };
        let deadline = drop_deadline_now() + DROP_SHUTDOWN_TIMEOUT;
        let _ = self
            .stopped
            .get_mut()
            .expect("fake-s3 completion mutex poisoned")
            .recv_timeout(deadline.saturating_duration_since(drop_deadline_now()));
        while !server_thread.is_finished() {
            let remaining = deadline.saturating_duration_since(drop_deadline_now());
            if remaining.is_zero() {
                // Dropping a JoinHandle detaches. The shutdown signal remains
                // set, but Drop itself must stay bounded if the thread wedges.
                return;
            }
            std::thread::sleep(remaining.min(Duration::from_millis(1)));
        }
        // Do not panic from defensive cleanup (especially during unwinding);
        // explicit shutdown reports a thread panic. `is_finished` makes this
        // join non-blocking even if completion was reported slightly early.
        let _ = server_thread.join();
    }
}

#[derive(Debug)]
struct ModelTimeSleep;

impl AsyncSleep for ModelTimeSleep {
    fn sleep(&self, duration: Duration) -> Sleep {
        Sleep::new(rt::sleep(duration))
    }
}

#[derive(Debug)]
struct ModelTimeSource;

impl TimeSource for ModelTimeSource {
    fn now(&self) -> std::time::SystemTime {
        rt::system_now()
    }
}

/// The accept loop, run on the dedicated server runtime. Binds an ephemeral
/// loopback port, reports it back over `addr_tx`, then serves each connection on
/// its own task.
async fn serve(
    state: Arc<FakeState>,
    conns: Option<Arc<AtomicU64>>,
    addr_tx: std::sync::mpsc::Sender<std::net::SocketAddr>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let socket = TcpSocket::new_v4().expect("create fake-s3 socket");
    socket
        .set_reuseaddr(true)
        .expect("allow fake-s3 listener reuse");
    socket.bind(addr).expect("bind fake-s3 socket");
    let listener = socket
        .listen(LISTEN_BACKLOG)
        .expect("listen fake-s3 socket");
    addr_tx.send(listener.local_addr().unwrap()).unwrap();
    let mut connections = JoinSet::new();
    loop {
        while connections.try_join_next().is_some() {}
        tokio::select! {
            biased;
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    continue;
                };
                if let Some(c) = &conns {
                    c.fetch_add(1, Ordering::Relaxed);
                }
                let io = TokioIo::new(stream);
                let st = state.clone();
                connections.spawn(async move {
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
    }
    connections.shutdown().await;
}
