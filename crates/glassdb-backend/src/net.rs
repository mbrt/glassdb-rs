//! A [`Backend`] transport over madsim's simulated network (`--cfg madsim`
//! only). It lets the deterministic-simulation harness place the object store on
//! its own node and reach it from each DB node over the network, so madsim's
//! network and node fault injection (clog/disconnect/kill) actually exercises
//! the storage path. See ADR-008.
//!
//! madsim's RPC is datagram-based: a clogged or disconnected link *drops*
//! packets rather than queuing them. Real object storage (S3/GCS) is instead a
//! reliable, highly-available service whose clients retry transient network
//! errors a *bounded* number of times and then surface an error (the AWS SDK's
//! adaptive retryer, for instance, gives up after a fixed attempt budget). We
//! model that here: [`NetBackend`] retries each call up to [`MAX_ATTEMPTS`]
//! times and then returns a transient error.
//!
//! Deliberately, there is *no* client-request dedup: [`serve_backend`] applies
//! every request it receives. Real object storage has no at-most-once request
//! id either, so a retry after a dropped *request* re-runs an op that never
//! landed, while a retry after a dropped *response* re-runs one that did. That
//! is sound because GlassDB's writes are conditional (CAS): re-running a write
//! whose first attempt already landed simply observes a `Precondition` failure
//! for its own write — exactly what S3 returns when the SDK retries a
//! conditional `PUT` whose first attempt succeeded but whose ack was lost. The
//! engine must resolve that ambiguity itself ("did my commit actually land?"),
//! and the simulator exposes it rather than papering over it.
//!
//! The net effect: a transient fault shorter than the retry budget appears as
//! latency (the call eventually lands), while a sustained outage surfaces as a
//! backend error that fails the transaction, leaving its write *in-doubt* —
//! counted as attempted but not acknowledged, like a node crash. The harness's
//! `acked <= final <= started` invariant tolerates in-doubt ops, and conditional
//! writes keep each one applied at most once (a re-delivered CAS write fails its
//! precondition).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::Ctx;
use madsim::net::rpc::Request;
use madsim::net::Endpoint;
use serde::{Deserialize, Serialize};

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// How long a single RPC attempt waits for a response before retrying. It only
/// matters under faults; it must comfortably exceed the worst-case round-trip so
/// that a retry never races a still-in-flight first attempt (which would let two
/// copies of the same op run concurrently). madsim's default per-hop latency is
/// 1-10ms (round-trip ~20ms), so 500ms of (virtual, free) time is a 25x margin
/// while still keeping the give-up budget short enough to be exercised.
const CALL_TIMEOUT: Duration = Duration::from_millis(500);
/// Backoff between retries after a dropped/timed-out call.
const RETRY_BACKOFF: Duration = Duration::from_millis(100);
/// Maximum attempts for a single logical operation before giving up with a
/// transient error. The resulting budget (~`MAX_ATTEMPTS * (CALL_TIMEOUT +
/// RETRY_BACKOFF)`, a few seconds of virtual time) comfortably outlasts a brief
/// fault so it appears as latency, while a longer outage surfaces as an error.
/// Each retry re-sends the same op and the server re-applies it; that is safe
/// because the engine's writes are conditional (a re-delivered CAS write fails
/// its precondition rather than applying twice).
const MAX_ATTEMPTS: u32 = 8;

/// The object-store operation carried by an RPC request. Mirrors the [`Backend`]
/// method set; versions are carried as their opaque token string and writer ids
/// as raw bytes to keep the wire types self-contained.
#[derive(Clone, Serialize, Deserialize)]
enum Op {
    ReadIfModified {
        path: String,
        writer: Vec<u8>,
    },
    Read {
        path: String,
    },
    GetMetadata {
        path: String,
    },
    SetTagsIf {
        path: String,
        expected: String,
        tags: Tags,
    },
    Write {
        path: String,
        value: Vec<u8>,
        tags: Tags,
    },
    WriteIf {
        path: String,
        value: Vec<u8>,
        expected: String,
        tags: Tags,
    },
    WriteIfNotExists {
        path: String,
        value: Vec<u8>,
        tags: Tags,
    },
    Delete {
        path: String,
    },
    DeleteIf {
        path: String,
        expected: String,
    },
    List {
        dir_path: String,
    },
}

/// An [`Op`] is itself the RPC request: every retry re-sends the same op, and
/// the server applies whatever it receives (no client-request dedup), so a
/// re-delivered op is re-run — see the module docs for why that is sound.
impl Request for Op {
    type Response = Resp;
    const ID: u64 = 0x_61DB_BE_C0DE;
}

/// The successful payload of a response, one variant per return shape.
#[derive(Clone, Serialize, Deserialize)]
enum RespBody {
    Read(ReadReply),
    Meta(Metadata),
    List(Vec<String>),
    Unit,
}

/// A response is the backend result of applying the request's [`Op`].
type Resp = Result<RespBody, BackendError>;

/// Applies a single [`Op`] to `inner`, wrapping the result into a [`Resp`].
async fn dispatch(inner: &dyn Backend, op: Op) -> Resp {
    let ctx = Ctx::background();
    match op {
        Op::ReadIfModified { path, writer } => inner
            .read_if_modified(&ctx, &path, &WriterId::new(writer))
            .await
            .map(RespBody::Read),
        Op::Read { path } => inner.read(&ctx, &path).await.map(RespBody::Read),
        Op::GetMetadata { path } => inner.get_metadata(&ctx, &path).await.map(RespBody::Meta),
        Op::SetTagsIf {
            path,
            expected,
            tags,
        } => inner
            .set_tags_if(&ctx, &path, &Version::new(expected), tags)
            .await
            .map(RespBody::Meta),
        Op::Write { path, value, tags } => inner
            .write(&ctx, &path, value, tags)
            .await
            .map(RespBody::Meta),
        Op::WriteIf {
            path,
            value,
            expected,
            tags,
        } => inner
            .write_if(&ctx, &path, value, &Version::new(expected), tags)
            .await
            .map(RespBody::Meta),
        Op::WriteIfNotExists { path, value, tags } => inner
            .write_if_not_exists(&ctx, &path, value, tags)
            .await
            .map(RespBody::Meta),
        Op::Delete { path } => inner.delete(&ctx, &path).await.map(|()| RespBody::Unit),
        Op::DeleteIf { path, expected } => inner
            .delete_if(&ctx, &path, &Version::new(expected))
            .await
            .map(|()| RespBody::Unit),
        Op::List { dir_path } => inner.list(&ctx, &dir_path).await.map(RespBody::List),
    }
}

/// Registers an RPC handler on `ep` that applies each received backend operation
/// to `inner`. There is no dedup: every request (including a client's retry) is
/// applied, mirroring object storage with no at-most-once request id. The
/// endpoint must already be bound, and must be kept alive for as long as the
/// server should accept requests.
pub fn serve_backend(ep: &Endpoint, inner: Arc<dyn Backend>) {
    ep.add_rpc_handler(move |op: Op| {
        let inner = inner.clone();
        async move { dispatch(inner.as_ref(), op).await }
    });
}

/// A [`Backend`] that forwards every call to a remote [`serve_backend`] over
/// madsim's network, retrying transient failures up to [`MAX_ATTEMPTS`] times so
/// brief network faults surface as latency, and returning a transient error once
/// the budget is exhausted (a sustained outage).
pub struct NetBackend {
    ep: Arc<Endpoint>,
    server: SocketAddr,
}

impl NetBackend {
    /// Creates a client targeting the `serve_backend` instance at `server`.
    pub fn new(ep: Arc<Endpoint>, server: SocketAddr) -> Self {
        NetBackend { ep, server }
    }

    /// Sends `op` and returns its response body, retrying dropped/timed-out
    /// calls up to [`MAX_ATTEMPTS`] times before giving up with a transient
    /// error (or returning early if `ctx` is cancelled). A retry re-sends the
    /// same op; the server has no dedup, so a retry after a dropped response
    /// re-runs it (safe because the engine's writes are conditional).
    async fn call(&self, ctx: &Ctx, op: Op) -> Result<RespBody, BackendError> {
        for attempt in 0..MAX_ATTEMPTS {
            if ctx.is_cancelled() {
                return Err(BackendError::Cancelled);
            }
            // `biased` keeps the poll order fixed; the default random order would
            // draw from a non-seeded RNG and break madsim's determinism.
            tokio::select! {
                biased;
                _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                res = self.ep.call_timeout(self.server, op.clone(), CALL_TIMEOUT) => {
                    if let Ok(resp) = res {
                        return resp;
                    }
                    // Dropped or timed out. If this was the last attempt, fall
                    // through to give up; otherwise back off and retry.
                    if attempt + 1 < MAX_ATTEMPTS {
                        tokio::select! {
                            biased;
                            _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                            _ = tokio::time::sleep(RETRY_BACKOFF) => {}
                        }
                    }
                }
            }
        }
        // Exhausted the attempt budget: the link is clogged or the peer is gone
        // for longer than a transient blip. Surface a non-precondition,
        // non-cancellation error so the transaction engine treats it as a real
        // (in-doubt) failure rather than a conflict to retry.
        Err(BackendError::Other(format!(
            "storage unavailable: no response after {MAX_ATTEMPTS} attempts"
        )))
    }
}

fn unexpected() -> BackendError {
    BackendError::Other("unexpected RPC response shape".into())
}

#[async_trait]
impl Backend for NetBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        match self
            .call(
                ctx,
                Op::ReadIfModified {
                    path: path.to_string(),
                    writer: expected_writer.as_bytes().to_vec(),
                },
            )
            .await?
        {
            RespBody::Read(r) => Ok(r),
            _ => Err(unexpected()),
        }
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        match self
            .call(
                ctx,
                Op::Read {
                    path: path.to_string(),
                },
            )
            .await?
        {
            RespBody::Read(r) => Ok(r),
            _ => Err(unexpected()),
        }
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        match self
            .call(
                ctx,
                Op::GetMetadata {
                    path: path.to_string(),
                },
            )
            .await?
        {
            RespBody::Meta(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self
            .call(
                ctx,
                Op::SetTagsIf {
                    path: path.to_string(),
                    expected: expected.token.clone(),
                    tags,
                },
            )
            .await?
        {
            RespBody::Meta(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self
            .call(
                ctx,
                Op::Write {
                    path: path.to_string(),
                    value,
                    tags,
                },
            )
            .await?
        {
            RespBody::Meta(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self
            .call(
                ctx,
                Op::WriteIf {
                    path: path.to_string(),
                    value,
                    expected: expected.token.clone(),
                    tags,
                },
            )
            .await?
        {
            RespBody::Meta(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        match self
            .call(
                ctx,
                Op::WriteIfNotExists {
                    path: path.to_string(),
                    value,
                    tags,
                },
            )
            .await?
        {
            RespBody::Meta(m) => Ok(m),
            _ => Err(unexpected()),
        }
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        match self
            .call(
                ctx,
                Op::Delete {
                    path: path.to_string(),
                },
            )
            .await?
        {
            RespBody::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        match self
            .call(
                ctx,
                Op::DeleteIf {
                    path: path.to_string(),
                    expected: expected.token.clone(),
                },
            )
            .await?
        {
            RespBody::Unit => Ok(()),
            _ => Err(unexpected()),
        }
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        match self
            .call(
                ctx,
                Op::List {
                    dir_path: dir_path.to_string(),
                },
            )
            .await?
        {
            RespBody::List(v) => Ok(v),
            _ => Err(unexpected()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBackend;
    use crate::LAST_WRITER_TAG;
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicBool, Ordering};

    use madsim::net::NetSim;
    use madsim::runtime::Runtime;

    const STORAGE: &str = "10.0.0.1:9000";
    const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 2);

    // Spawns a storage server on its own node, returning the node and a flag
    // that flips once the server is bound and ready to accept requests.
    fn spawn_server(
        rt: &Runtime,
        inner: Arc<dyn Backend>,
    ) -> (madsim::runtime::NodeHandle, Arc<AtomicBool>) {
        let ready = Arc::new(AtomicBool::new(false));
        let node = rt.create_node().ip("10.0.0.1".parse().unwrap()).build();
        let r = ready.clone();
        node.spawn(async move {
            let ep = Endpoint::bind(STORAGE).await.unwrap();
            serve_backend(&ep, inner);
            r.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        });
        (node, ready)
    }

    async fn wait_ready(ready: &AtomicBool) {
        while !ready.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[test]
    fn round_trips_every_op() {
        let rt = Runtime::new();
        let (_storage, ready) = spawn_server(&rt, Arc::new(MemoryBackend::new()));
        let client = rt.create_node().ip(CLIENT_IP.into()).build();

        let f = client.spawn(async move {
            wait_ready(&ready).await;
            let ep = Arc::new(Endpoint::bind((CLIENT_IP, 0)).await.unwrap());
            let be = NetBackend::new(ep, STORAGE.parse().unwrap());
            let ctx = Ctx::background();

            assert!(matches!(
                be.read(&ctx, "a").await,
                Err(BackendError::NotFound)
            ));
            let m = be
                .write(&ctx, "a", b"hello".to_vec(), Tags::new())
                .await
                .unwrap();
            let r = be.read(&ctx, "a").await.unwrap();
            assert_eq!(r.contents, b"hello");
            assert_eq!(r.version, m.version);

            // Conditional write on the wrong version fails; on the right one
            // succeeds.
            assert!(matches!(
                be.write_if(&ctx, "a", b"x".to_vec(), &Version::new("9/9"), Tags::new())
                    .await,
                Err(BackendError::Precondition)
            ));
            let m2 = be
                .write_if(&ctx, "a", b"world".to_vec(), &m.version, Tags::new())
                .await
                .unwrap();
            assert_ne!(m.version, m2.version);

            // Metadata, tags, and read_if_modified.
            let writer = WriterId::new(vec![1, 2, 3]);
            let mut tags = Tags::new();
            tags.insert(
                LAST_WRITER_TAG.to_string(),
                crate::encode_writer_tag(&writer),
            );
            let m3 = be.set_tags_if(&ctx, "a", &m2.version, tags).await.unwrap();
            assert!(be.get_metadata(&ctx, "a").await.unwrap().version == m3.version);
            assert!(matches!(
                be.read_if_modified(&ctx, "a", &writer).await,
                Err(BackendError::Precondition)
            ));

            // Listing and deletion.
            be.write(&ctx, "d/x", b"1".to_vec(), Tags::new())
                .await
                .unwrap();
            be.write(&ctx, "d/y", b"2".to_vec(), Tags::new())
                .await
                .unwrap();
            assert_eq!(
                be.list(&ctx, "d").await.unwrap(),
                vec!["d/x".to_string(), "d/y".to_string()]
            );
            be.delete(&ctx, "a").await.unwrap();
            assert!(matches!(
                be.delete(&ctx, "a").await,
                Err(BackendError::NotFound)
            ));
        });
        rt.block_on(f).unwrap();
    }

    #[test]
    fn clogged_link_recovers_after_unclog() {
        let rt = Runtime::new();
        let (storage, ready) = spawn_server(&rt, Arc::new(MemoryBackend::new()));
        let client = rt.create_node().ip(CLIENT_IP.into()).build();
        let storage_id = storage.id();
        let client_id = client.id();

        // Drive the fault from the main task: clog client->storage, kick off a
        // write that must keep retrying, then unclog and confirm it lands.
        let handle = client.spawn(async move {
            wait_ready(&ready).await;
            let ep = Arc::new(Endpoint::bind((CLIENT_IP, 0)).await.unwrap());
            let be = NetBackend::new(ep, STORAGE.parse().unwrap());
            let ctx = Ctx::background();
            be.write(&ctx, "k", b"v".to_vec(), Tags::new())
                .await
                .unwrap();
            be.read(&ctx, "k").await.unwrap().contents
        });

        let got = rt.block_on(async move {
            let net = NetSim::current();
            net.clog_link(client_id, storage_id);
            // Let virtual time pass (but stay within the retry budget) while the
            // client retries against the clog, then heal it before it gives up.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(!handle.is_finished(), "write should still be retrying");
            net.unclog_link(client_id, storage_id);
            handle.await.unwrap()
        });
        assert_eq!(got, b"v");
    }

    #[test]
    fn permanent_clog_gives_up() {
        let rt = Runtime::new();
        let (storage, ready) = spawn_server(&rt, Arc::new(MemoryBackend::new()));
        let client = rt.create_node().ip(CLIENT_IP.into()).build();
        let storage_id = storage.id();
        let client_id = client.id();

        // A link that never heals: the client should exhaust its attempt budget
        // and surface a transient error rather than retry forever.
        let handle = client.spawn(async move {
            wait_ready(&ready).await;
            let ep = Arc::new(Endpoint::bind((CLIENT_IP, 0)).await.unwrap());
            let be = NetBackend::new(ep, STORAGE.parse().unwrap());
            let ctx = Ctx::background();
            be.write(&ctx, "k", b"v".to_vec(), Tags::new()).await
        });

        let err = rt.block_on(async move {
            NetSim::current().clog_link(client_id, storage_id);
            handle.await.unwrap()
        });
        assert!(
            matches!(err, Err(BackendError::Other(_))),
            "expected a transient give-up error, got {err:?}"
        );
    }
}
