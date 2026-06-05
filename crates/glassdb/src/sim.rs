//! Deterministic-simulation workload harness for the concurrency fuzzer.
//!
//! A [`Workload`] is a set of clients, each a sequence of [`Op`]s, generated
//! from fuzzer bytes via [`arbitrary`]. Under the madsim simulator (`--cfg
//! madsim`) the harness builds a small cluster: the object store runs on its own
//! node, and each client opens a [`DB`] on its own node that reaches the store
//! over the simulated network (see [`glassdb_backend::net`]). A seeded
//! [`FaultConfig`]-driven nemesis then injects network faults (clog/partition)
//! and node faults (pause/crash) while the clients run. Scheduling, time,
//! randomness, and the fault schedule are all functions of a single seed, so a
//! failing run reproduces exactly from its input.
//!
//! The correctness check is per key `acked <= final <= started`, where `started`
//! counts increments that entered a transaction and `acked` counts those whose
//! commit returned `Ok`. An increment is left in-doubt (counted in `started`,
//! not `acked`) when a client crashes mid-commit or a sustained network outage
//! exhausts NetBackend's retry budget and fails the transaction; the bound
//! tolerates both while still catching lost or fabricated writes (conditional
//! writes keep each in-doubt op applied at most once, even when a retry
//! re-delivers it). With faults disabled the three are equal, recovering the
//! original exact invariant. [`run_and_record`] also
//! captures the ordered stream of backend operations so two same-seed runs can
//! be compared byte-for-byte (see the `concurrent_sim` self-check and ADR-008).

use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{OpLog, RecordingBackend};
use glassdb_backend::Backend;

use crate::{Collection, Ctx, Error, Options, DB};

#[cfg(madsim)]
use glassdb_backend::net::{serve_backend, NetBackend};
#[cfg(madsim)]
use madsim::net::{Endpoint, NetSim};
#[cfg(madsim)]
use madsim::runtime::Handle;
#[cfg(madsim)]
use madsim::task::NodeId;
#[cfg(madsim)]
use std::net::{IpAddr, SocketAddr};

/// Number of distinct keys the workload operates on.
pub const KEY_COUNT: usize = 4;
const MAX_CLIENTS: usize = 4;
const MAX_OPS_PER_CLIENT: usize = 8;
const COLLECTION: &[u8] = b"fuzz";
const DB_NAME: &str = "fuzz";

/// A single operation performed by a client, all wrapped in their own
/// transaction (with automatic conflict retries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Read-modify-write: increment a single key.
    Rmw(usize),
    /// Increment two distinct keys in the same transaction.
    MultiRmw(usize, usize),
    /// Read-only transaction over a set of keys.
    ReadOnly(Vec<usize>),
}

/// A complete workload: one op sequence per client. Clients run concurrently.
#[derive(Debug, Clone, Default)]
pub struct Workload {
    /// Per-client op sequences.
    pub clients: Vec<Vec<Op>>,
}

/// Controls the fault nemesis. With `enabled` false the harness still places
/// each DB on its own node but injects no faults (so the exact-count invariant
/// holds); `intensity` scales how many fault rounds the nemesis performs.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultConfig {
    /// Whether the nemesis injects network and node faults.
    pub enabled: bool,
    /// How aggressive the nemesis is (number of fault rounds scales with this).
    pub intensity: u8,
}

impl FaultConfig {
    /// No fault injection (each DB still runs on its own node).
    pub fn none() -> Self {
        Self::default()
    }

    /// Fault injection enabled at the given intensity.
    pub fn enabled(intensity: u8) -> Self {
        FaultConfig {
            enabled: true,
            intensity,
        }
    }
}

impl<'a> Arbitrary<'a> for FaultConfig {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(FaultConfig {
            enabled: u.arbitrary()?,
            intensity: u.arbitrary()?,
        })
    }
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let key = |u: &mut Unstructured<'a>| -> arbitrary::Result<usize> {
            Ok(u.arbitrary::<u8>()? as usize % KEY_COUNT)
        };
        Ok(match u.arbitrary::<u8>()? % 3 {
            0 => Op::Rmw(key(u)?),
            1 => {
                let a = key(u)?;
                // Force the second key to differ so the increment count is
                // unambiguous (two writes to the same key in one tx net +1).
                let b = (a + 1 + (u.arbitrary::<u8>()? as usize % (KEY_COUNT - 1))) % KEY_COUNT;
                Op::MultiRmw(a, b)
            }
            _ => {
                let n = u.arbitrary::<u8>()? as usize % (KEY_COUNT + 1);
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(key(u)?);
                }
                Op::ReadOnly(keys)
            }
        })
    }
}

impl<'a> Arbitrary<'a> for Workload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for _ in 0..nclients {
            let nops = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut ops = Vec::with_capacity(nops);
            for _ in 0..nops {
                ops.push(Op::arbitrary(u)?);
            }
            clients.push(ops);
        }
        Ok(Workload { clients })
    }
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

fn key_name(k: usize) -> Vec<u8> {
    format!("k{k}").into_bytes()
}

async fn read_int_from_tx(tx: &crate::Tx, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(v) => Ok(read_int(&v)),
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

/// Per-key accounting shared across client tasks (and surviving node crashes via
/// the harness-owned `Arc`). `started` counts increments that entered a
/// transaction; `acked` counts those whose commit returned `Ok`.
struct Acct {
    started: Vec<i64>,
    acked: Vec<i64>,
}

impl Acct {
    fn new() -> Self {
        Acct {
            started: vec![0; KEY_COUNT],
            acked: vec![0; KEY_COUNT],
        }
    }
}

/// Runs a client's op sequence, updating `acct` as each increment is attempted
/// and acknowledged. Returns on the first transaction error, leaving that op
/// counted in `started` but not `acked` (i.e. in-doubt).
async fn run_client(
    ctx: &Ctx,
    db: &DB,
    coll: &Collection,
    ops: &[Op],
    acct: &Mutex<Acct>,
) -> Result<(), Error> {
    for op in ops {
        match op {
            Op::Rmw(k) => {
                acct.lock().unwrap().started[*k] += 1;
                // Bind a reference so the `async move` body captures `&Vec`
                // (`Copy`) instead of moving the key, keeping the closure
                // `FnMut` for retries.
                let kn = &key_name(*k);
                db.tx(ctx, |tx| async move {
                    let cur = read_int_from_tx(&tx, coll, kn).await?;
                    tx.write(coll, kn, &write_int(cur + 1))
                })
                .await?;
                acct.lock().unwrap().acked[*k] += 1;
            }
            Op::MultiRmw(a, b) => {
                {
                    let mut g = acct.lock().unwrap();
                    g.started[*a] += 1;
                    g.started[*b] += 1;
                }
                let ka = &key_name(*a);
                let kb = &key_name(*b);
                db.tx(ctx, |tx| async move {
                    let va = read_int_from_tx(&tx, coll, ka).await?;
                    let vb = read_int_from_tx(&tx, coll, kb).await?;
                    tx.write(coll, ka, &write_int(va + 1))?;
                    tx.write(coll, kb, &write_int(vb + 1))
                })
                .await?;
                {
                    let mut g = acct.lock().unwrap();
                    g.acked[*a] += 1;
                    g.acked[*b] += 1;
                }
            }
            Op::ReadOnly(keys) => {
                let names: Vec<Vec<u8>> = keys.iter().map(|k| key_name(*k)).collect();
                let names = &names;
                db.tx(ctx, |tx| async move {
                    for kn in names {
                        let v = read_int_from_tx(&tx, coll, kn).await?;
                        assert!(v >= 0, "observed negative value {v} for key {kn:?}");
                    }
                    Ok(())
                })
                .await?;
            }
        }
    }
    Ok(())
}

/// Total increments each key should receive, derived from the workload.
fn expected_increments(workload: &Workload) -> Vec<i64> {
    let mut expected = vec![0i64; KEY_COUNT];
    for ops in &workload.clients {
        for op in ops {
            match op {
                Op::Rmw(k) => expected[*k] += 1,
                Op::MultiRmw(a, b) => {
                    expected[*a] += 1;
                    expected[*b] += 1;
                }
                Op::ReadOnly(_) => {}
            }
        }
    }
    expected
}

fn deterministic_options() -> Options {
    Options {
        deterministic_time: true,
        ..Default::default()
    }
}

/// Asserts the serializability invariant for every key: an acknowledged commit
/// must be durable (`acked <= final`) and the store cannot show more than what
/// was attempted (`final <= started`). With faults disabled, `final` must equal
/// the workload's total increments exactly.
fn assert_bounds(acct: &Acct, finals: &[i64], expected: &[i64], faults_enabled: bool) {
    for k in 0..KEY_COUNT {
        let a = acct.acked[k];
        let s = acct.started[k];
        let f = finals[k];
        assert!(
            a <= f && f <= s,
            "key k{k}: violated acked({a}) <= final({f}) <= started({s})"
        );
        if !faults_enabled {
            assert_eq!(
                f, expected[k],
                "key k{k}: final {f} != expected {} (no faults)",
                expected[k]
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Non-madsim path: a single shared in-process backend, clients on one task. The
// network/node fault machinery requires the simulator, so this path runs
// fault-free and keeps the exact invariant. It exists so the `sim` harness still
// compiles and runs in a normal build.
// ---------------------------------------------------------------------------

#[cfg(not(madsim))]
async fn run_on(workload: &Workload, backend: Arc<dyn Backend>) {
    let ctx = Ctx::background();
    let opts = deterministic_options();

    // Initialize the collection and seed every key to zero up front.
    let init_db = DB::open_with(&ctx, DB_NAME, backend.clone(), opts.clone())
        .await
        .expect("open init db");
    let init_coll = init_db.collection(COLLECTION);
    init_coll.create(&ctx).await.expect("create collection");
    let seed_coll = &init_coll;
    init_db
        .tx(&ctx, |tx| async move {
            for k in 0..KEY_COUNT {
                tx.write(seed_coll, &key_name(k), &write_int(0))?;
            }
            Ok(())
        })
        .await
        .expect("seed keys");

    let expected = expected_increments(workload);
    let acct = Arc::new(Mutex::new(Acct::new()));

    let mut futs = Vec::with_capacity(workload.clients.len());
    for ops in &workload.clients {
        let db = DB::open_with(&ctx, DB_NAME, backend.clone(), opts.clone())
            .await
            .expect("open client db");
        let ops = ops.clone();
        let cctx = ctx.clone();
        let acct = acct.clone();
        futs.push(async move {
            let coll = db.collection(COLLECTION);
            let res = run_client(&cctx, &db, &coll, &ops, &acct).await;
            db.close().await;
            res
        });
    }
    for res in futures::future::join_all(futs).await {
        res.expect("client tx failed");
    }

    let mut finals = vec![0i64; KEY_COUNT];
    for (k, slot) in finals.iter_mut().enumerate() {
        *slot = read_int(
            &init_coll
                .read_strong(&ctx, &key_name(k))
                .await
                .expect("final read"),
        );
    }
    init_db.close().await;

    let acct = acct.lock().unwrap();
    assert_bounds(&acct, &finals, &expected, false);
}

// ---------------------------------------------------------------------------
// madsim path: a storage node serving the backend over the network, one DB per
// client node, and a seeded fault nemesis.
// ---------------------------------------------------------------------------

#[cfg(madsim)]
const SERVER_ADDR: &str = "10.0.0.1:9000";

/// Opens a [`DB`] backed by a fresh network client bound to `ip`, talking to the
/// storage node. Open performs backend I/O, so it can fail if a sustained
/// network fault is active; callers that run while the nemesis is live must
/// tolerate the error.
#[cfg(madsim)]
async fn open_net_db(ip: IpAddr) -> Result<(DB, Ctx), Error> {
    let ep = Arc::new(
        Endpoint::bind(format!("{ip}:0"))
            .await
            .expect("bind client endpoint"),
    );
    let server: SocketAddr = SERVER_ADDR.parse().unwrap();
    let backend: Arc<dyn Backend> = Arc::new(NetBackend::new(ep, server));
    let ctx = Ctx::background();
    let db = DB::open_with(&ctx, DB_NAME, backend, deterministic_options()).await?;
    Ok((db, ctx))
}

#[cfg(madsim)]
async fn run_nodes(workload: &Workload, storage: Arc<dyn Backend>, faults: FaultConfig) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::sync::oneshot;

    let handle = Handle::current();
    let server: SocketAddr = SERVER_ADDR.parse().unwrap();

    // Storage node: serves the backend for the whole run. Its state lives in the
    // harness-owned `storage` Arc, so it is durable independent of any node.
    let storage_node = handle.create_node().ip("10.0.0.1".parse().unwrap()).build();
    let ready = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let storage_task = {
        let storage = storage.clone();
        let ready = ready.clone();
        storage_node.spawn(async move {
            let ep = Endpoint::bind(server).await.expect("bind storage");
            serve_backend(&ep, storage);
            ready.store(true, Ordering::SeqCst);
            // Keep the endpoint (and the bound socket) alive until the harness
            // signals shutdown, then drop it so the RPC handler and the backend
            // are released. Parking it forever would leak those allocations (the
            // fuzzer runs under a leak sanitizer).
            let _ = shutdown_rx.await;
        })
    };
    while !ready.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Every node we create is torn down at the end of the run. madsim's
    // `block_on` returns as soon as this (main) task finishes, so any
    // background task still parked on a timer or lock wait would otherwise be
    // abandoned and leaked on runtime drop.
    let mut all_nodes: Vec<NodeId> = vec![storage_node.id()];

    // Init node: create the collection and seed every key to zero.
    let init_node = handle.create_node().ip("10.0.0.2".parse().unwrap()).build();
    all_nodes.push(init_node.id());
    init_node
        .spawn(async move {
            // The nemesis has not started yet, so open and seed cannot hit a
            // fault; treat any failure here as a harness bug.
            let (db, ctx) = open_net_db("10.0.0.2".parse().unwrap())
                .await
                .expect("open init db");
            let coll = db.collection(COLLECTION);
            coll.create(&ctx).await.expect("create collection");
            let seed = &coll;
            db.tx(&ctx, |tx| async move {
                for k in 0..KEY_COUNT {
                    tx.write(seed, &key_name(k), &write_int(0))?;
                }
                Ok(())
            })
            .await
            .expect("seed keys");
            db.close().await;
        })
        .await
        .expect("init task");

    let expected = expected_increments(workload);
    let acct = Arc::new(Mutex::new(Acct::new()));

    // Client nodes: one DB per client, each on its own node.
    let mut client_ids: Vec<NodeId> = Vec::new();
    let mut client_handles = Vec::new();
    for (i, ops) in workload.clients.iter().enumerate() {
        let ip: IpAddr = format!("10.0.1.{}", i + 1).parse().unwrap();
        let node = handle.create_node().ip(ip).build();
        client_ids.push(node.id());
        all_nodes.push(node.id());
        let ops = ops.clone();
        let acct = acct.clone();
        client_handles.push(node.spawn(async move {
            // A sustained outage can make open itself fail; that client simply
            // does no work (no increments started), which the bound tolerates.
            let Ok((db, ctx)) = open_net_db(ip).await else {
                return;
            };
            let coll = db.collection(COLLECTION);
            let _ = run_client(&ctx, &db, &coll, &ops, &acct).await;
            db.close().await;
        }));
    }

    // Nemesis: inject faults concurrently while the clients run.
    let stop = Arc::new(AtomicBool::new(false));
    let nemesis_handle = if faults.enabled {
        let h = handle.clone();
        let ids = client_ids.clone();
        let storage_id = storage_node.id();
        let stop = stop.clone();
        let intensity = faults.intensity;
        let nemesis_node = handle.create_node().build();
        all_nodes.push(nemesis_node.id());
        Some(nemesis_node.spawn(async move { nemesis(h, ids, storage_id, intensity, stop).await }))
    } else {
        None
    };

    // Wait for the clients. A killed node yields a `JoinError`; that is an
    // expected crash, not a failure.
    for h in client_handles {
        let _ = h.await;
    }
    stop.store(true, Ordering::SeqCst);
    if let Some(h) = nemesis_handle {
        let _ = h.await;
    }

    // Defensive: ensure no network fault outlives the workload before verifying.
    let net = NetSim::current();
    let storage_id = storage_node.id();
    for &c in &client_ids {
        net.unclog_node(c);
        net.unclog_link(c, storage_id);
        net.unclog_link(storage_id, c);
    }

    // Verifier node: read every key's final value (driving recovery of any
    // crashed client's locks via lease expiry) and check the invariant.
    let verifier_node = handle.create_node().ip("10.0.0.3".parse().unwrap()).build();
    all_nodes.push(verifier_node.id());
    let finals = verifier_node
        .spawn(async move {
            // All faults are healed before the verifier runs, so open is safe.
            let (db, ctx) = open_net_db("10.0.0.3".parse().unwrap())
                .await
                .expect("open verifier db");
            let coll = db.collection(COLLECTION);
            let mut finals = vec![0i64; KEY_COUNT];
            for (k, slot) in finals.iter_mut().enumerate() {
                *slot = read_int(
                    &coll
                        .read_strong(&ctx, &key_name(k))
                        .await
                        .expect("final read"),
                );
            }
            db.close().await;
            finals
        })
        .await
        .expect("verifier task");

    // Shut the storage server down and join it so its endpoint, RPC handler,
    // and backend are all released before the run ends.
    let _ = shutdown_tx.send(());
    let _ = storage_task.await;

    // Tear down every node so madsim drops any background task still parked on
    // a timer or lock wait (locker/monitor/gc helpers spawned detached). In a
    // real tokio runtime these are reclaimed when the runtime shuts down; under
    // madsim they must be killed explicitly or they leak. Killing wakes the
    // tasks; yielding lets the executor pull and drop them before we return.
    for &id in &all_nodes {
        handle.kill(id);
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }

    let acct = acct.lock().unwrap();
    assert_bounds(&acct, &finals, &expected, faults.enabled);
}

/// The fault nemesis. Drives a seeded sequence of network and node faults
/// against the client nodes while they run, eventually healing every network
/// fault so the run can make progress. A short fault appears to the client as
/// latency; a long one outlasts NetBackend's retry budget, so the in-flight call
/// gives up and the transaction fails in-doubt (recovered later via lock-lease
/// expiry, just like a crash). Crashes (`kill`) are permanent; their recovery is
/// likewise left to lease expiry. Randomness comes from the runtime's seeded RNG
/// and all timing from virtual sleeps, so the schedule is a deterministic
/// function of the seed.
#[cfg(madsim)]
async fn nemesis(
    handle: Handle,
    clients: Vec<NodeId>,
    storage: NodeId,
    intensity: u8,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    use madsim::rand::Rng;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let net = NetSim::current();
    let mut alive = clients;
    let rounds = 1 + (intensity as usize % 6);
    for _ in 0..rounds {
        if stop.load(Ordering::SeqCst) || alive.is_empty() {
            break;
        }
        // Draw all randomness up front so no RNG guard is held across an await.
        // `hold_ms`'s upper bound exceeds NetBackend's retry budget (a few
        // seconds) on purpose: most faults heal within the budget and surface as
        // latency, but the longest ones outlast it so the client gives up and
        // the transaction fails in-doubt, exercising the backend-error path.
        let (idx, kind, hold_ms, gap_ms) = {
            let mut rng = madsim::rand::thread_rng();
            (
                rng.gen_range(0..alive.len()),
                rng.gen_range(0u32..10),
                rng.gen_range(50u64..6000),
                rng.gen_range(10u64..400),
            )
        };
        let target = alive[idx];
        match kind {
            0..=3 => {
                // Partition the client from storage (both directions).
                net.clog_link(target, storage);
                net.clog_link(storage, target);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                net.unclog_link(target, storage);
                net.unclog_link(storage, target);
            }
            4..=5 => {
                // Fully clog the node's traffic, then restore it.
                net.clog_node(target);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                net.unclog_node(target);
            }
            6..=7 => {
                // Stall the node's execution, then resume it.
                handle.pause(target);
                tokio::time::sleep(Duration::from_millis(hold_ms)).await;
                handle.resume(target);
            }
            _ => {
                // Crash the node (no restart): its in-flight work is in-doubt.
                handle.kill(target);
                alive.swap_remove(idx);
            }
        }
        tokio::time::sleep(Duration::from_millis(gap_ms)).await;
    }
}

// ---------------------------------------------------------------------------
// Public entry points. Under madsim these run the node cluster; otherwise the
// single-process fallback.
// ---------------------------------------------------------------------------

/// Runs `workload` over a fresh in-memory store and asserts serializability,
/// without injecting faults.
pub async fn run_and_assert(workload: Workload) {
    run_and_assert_with_faults(workload, FaultConfig::none()).await;
}

/// Like [`run_and_assert`] but injects the network and node faults described by
/// `faults`. Outside the madsim simulator `faults` is ignored (the fault
/// machinery needs the simulator).
pub async fn run_and_assert_with_faults(workload: Workload, faults: FaultConfig) {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    #[cfg(madsim)]
    {
        run_nodes(&workload, backend, faults).await;
    }
    #[cfg(not(madsim))]
    {
        let _ = faults;
        run_on(&workload, backend).await;
    }
}

/// Like [`run_and_assert`] but records the ordered stream of backend operations
/// and returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_and_record(workload: &Workload) -> OpLog {
    run_and_record_with_faults(workload, FaultConfig::none()).await
}

/// Like [`run_and_record`] but with fault injection enabled per `faults`.
pub async fn run_and_record_with_faults(workload: &Workload, faults: FaultConfig) -> OpLog {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let rec = Arc::new(RecordingBackend::new(mem));
    let log = rec.log();
    let backend: Arc<dyn Backend> = rec;
    #[cfg(madsim)]
    {
        run_nodes(workload, backend, faults).await;
    }
    #[cfg(not(madsim))]
    {
        let _ = faults;
        run_on(workload, backend).await;
    }
    log
}
