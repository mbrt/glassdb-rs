//! Deterministic-simulation workload harness for the concurrency fuzzer.
//!
//! A [`Workload`] is a set of clients, each a sequence of [`Op`]s, generated
//! from fuzzer bytes via [`arbitrary`]. The harness runs every client as its own
//! task over a shared in-process [`MemoryBackend`]; under the deterministic
//! simulation executor (`--cfg sim`) the executor's scheduler controls how those
//! tasks interleave, so the whole run is a pure function of the schedule tape and
//! the seed and a failing run reproduces exactly. A seeded [`FaultConfig`] gives
//! each client its own [`FaultBackend`] *transport* to the shared store, which
//! injects latency and faults the client's ops on either side (dropped request /
//! lost ack) including sustained per-client outages; a crash nemesis cancels
//! client contexts mid-flight (modelling an abrupt client stop), after which the
//! client restarts on the same backend.
//!
//! The correctness check is per key `acked <= final <= started`, where `started`
//! counts increments that entered a transaction and `acked` counts those whose
//! commit returned `Ok`. An increment is left in-doubt (counted in `started`,
//! not `acked`) when a client is cancelled mid-commit or a conditional write's
//! outcome cannot be confirmed (its acknowledgement was lost). In every case the
//! engine surfaces the failure to the caller and does *not* retry the
//! transaction transparently, so each op is applied at most once; the bound
//! tolerates the in-doubt op while still catching lost or fabricated writes. With
//! faults disabled the three are equal, recovering the exact invariant.
//! [`run_and_record`] also captures the ordered stream of backend operations so
//! two same-seed/tape runs can be compared byte-for-byte (see the
//! `concurrent_sim` self-check and ADR-010/011).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
#[cfg(sim)]
use glassdb_backend::middleware::OpRecord;
use glassdb_backend::middleware::{FaultBackend, FaultOptions, OpLog, RecordingBackend};
use glassdb_concurr::{Tape, rt};
use tokio_util::sync::CancellationToken;

use crate::{Collection, Database, Error};

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

/// Controls the fault nemesis. With `enabled` false the harness injects no
/// faults (so the exact-count invariant holds); `intensity` scales both the
/// [`FaultBackend`] probabilities and how many clients the crash nemesis cancels.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultConfig {
    /// Whether the harness injects backend faults and client crashes.
    pub enabled: bool,
    /// How aggressive the faults are (probabilities and crash count scale with
    /// this).
    pub intensity: u8,
}

impl FaultConfig {
    /// No fault injection.
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

async fn read_int_from_tx(tx: &crate::Transaction, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(v) => Ok(read_int(&v)),
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

/// Per-key accounting shared across client tasks. `started` counts increments
/// that entered a transaction; `acked` counts those whose commit returned `Ok`.
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

/// Attempts a single op in its own transaction, updating `acct`: `started` is
/// bumped before the attempt, `acked` only on success. A failure leaves the op
/// counted in `started` but not `acked` (i.e. in-doubt).
async fn run_one(
    db: &Database,
    coll: &Collection,
    op: &Op,
    acct: &Mutex<Acct>,
) -> Result<(), Error> {
    match op {
        Op::Rmw(k) => {
            acct.lock().unwrap().started[*k] += 1;
            // Bind a reference so the `async move` body captures `&Vec`
            // (`Copy`) instead of moving the key, keeping the closure
            // `FnMut` for retries.
            let kn = &key_name(*k);
            db.tx(|tx| async move {
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
            db.tx(|tx| async move {
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
            db.tx(|tx| async move {
                for kn in names {
                    let v = read_int_from_tx(&tx, coll, kn).await?;
                    assert!(v >= 0, "observed negative value {v} for key {kn:?}");
                }
                Ok(())
            })
            .await?;
        }
    }
    Ok(())
}

/// Runs a client's op sequence in order. Returns the number of ops *consumed*:
/// `ops.len()` if all succeeded, or `i + 1` if op `i` failed (that op is left
/// in-doubt and is *not* replayed on restart, since re-running a non-idempotent
/// RMW would double-apply).
async fn run_client(
    db: &Database,
    coll: &Collection,
    ops: &[Op],
    acct: &Mutex<Acct>,
    consumed: &AtomicUsize,
) -> usize {
    for (i, op) in ops.iter().enumerate() {
        // Bump consumed *before* attempting the op. If the outer crash future
        // drops us mid-`run_one`, this op is counted as consumed (left in
        // doubt) and is not replayed by the restart path. We need this counter
        // on a shared atomic because the `tokio::select!` cancel arm simply
        // drops this future and cannot read its return value.
        consumed.store(i + 1, Ordering::SeqCst);
        if run_one(db, coll, op, acct).await.is_err() {
            return i + 1;
        }
    }
    consumed.store(ops.len(), Ordering::SeqCst);
    ops.len()
}

/// Opens the Database on `backend`, runs `ops`, and closes it. Returns ops consumed
/// (see [`run_client`]); `0` if the open itself failed (e.g. a crash during
/// startup).
async fn open_and_run(
    backend: &Arc<dyn Backend>,
    ops: &[Op],
    acct: &Mutex<Acct>,
    consumed: &AtomicUsize,
) {
    let Ok(db) = open_det_db(backend).await else {
        return;
    };
    let coll = db.collection(COLLECTION);
    let _ = run_client(&db, &coll, ops, acct, consumed).await;
    db.shutdown().await;
}

/// Opens the simulation Database with the deterministic clock the fuzzer relies on
/// for byte-identical replays.
async fn open_det_db(backend: &Arc<dyn Backend>) -> Result<Database, Error> {
    Database::builder(DB_NAME, backend.clone())
        .deterministic_time(true)
        .open()
        .await
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

/// The crash nemesis: cancels a few clients' abort signals at deterministic
/// virtual times, modelling an abrupt client stop mid-transaction. Each cancelled
/// client drops its `Database` (without calling `shutdown`), so its in-flight locks
/// are recovered later via lease expiry during the final reads. Crash timing and
/// targets are drawn from `tape` (the fuzzer-guided fault schedule), falling back
/// to the tape's seeded PRNG once its bytes run out.
async fn crash_nemesis(signals: Vec<CancellationToken>, intensity: u8, mut tape: Tape) {
    let crashes = (intensity as usize % 3).min(signals.len());
    for _ in 0..crashes {
        let gap = tape.below(40) + 1;
        rt::sleep(Duration::from_millis(gap)).await;
        let idx = tape.below(signals.len() as u64) as usize;
        signals[idx].cancel();
    }
}

/// Number of sustained outage windows the outage nemesis opens at `intensity`.
fn outage_count(intensity: u8) -> usize {
    match intensity {
        0..=47 => 0,
        48..=127 => 1,
        _ => 2,
    }
}

/// The outage nemesis: takes a whole client's transport down for a sustained
/// span and then heals it, modelling a node that disconnects/clogs and later
/// recovers. While one client is down its peers keep reaching storage and can
/// recover its orphaned locks via lease expiry — the path coincident i.i.d.
/// rolls reach only by luck. The target client, start gap, and duration are
/// drawn from `tape` (the fuzzer-guided fault schedule).
async fn outage_nemesis(transports: Vec<Arc<FaultBackend>>, intensity: u8, mut tape: Tape) {
    if transports.is_empty() {
        return;
    }
    for _ in 0..outage_count(intensity) {
        let gap = tape.below(30) + 1;
        rt::sleep(Duration::from_millis(gap)).await;
        let idx = tape.below(transports.len() as u64) as usize;
        transports[idx].down();
        // Sustained: long enough that retries during the window keep failing,
        // so recovery happens via lease expiry rather than a lucky retry.
        let span = tape.below(80) + 20;
        rt::sleep(Duration::from_millis(span)).await;
        transports[idx].heal();
    }
}

/// Deinterleaves a fault tape into `N` independent byte streams (byte `i` goes to
/// stream `i % N`). Keeping the streams disjoint means a single mutated byte maps
/// to exactly one fault decision, which is what makes the fault schedule
/// coverage-guidable.
fn deinterleave<const N: usize>(tape: &[u8]) -> [Vec<u8>; N] {
    let mut out: [Vec<u8>; N] = std::array::from_fn(|_| Vec::new());
    for (i, &b) in tape.iter().enumerate() {
        out[i % N].push(b);
    }
    out
}

// Fault-tape stream layout: one stream for each nemesis, plus one per client
// transport (so each client's faults are guided by its own disjoint bytes).
const CRASH_STREAM: usize = 0;
const OUTAGE_STREAM: usize = 1;
const CLIENT_STREAM_BASE: usize = 2;
const FAULT_STREAMS: usize = CLIENT_STREAM_BASE + MAX_CLIENTS;

/// Distinct PRNG-fallback seed for client `i`'s transport, so an exhausted tape
/// does not make every client fault in lockstep.
fn client_seed(seed: u64, i: usize) -> u64 {
    seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i as u64 + 1)
}

/// The shared faultless backbone every client reaches through its own
/// transport: a `MemoryBackend` behind a `RecordingBackend` whose ordered op log
/// powers the byte-for-byte determinism self-check. Init and verification use it
/// directly (a perfect connection).
fn make_backbone() -> (Arc<dyn Backend>, OpLog) {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let rec = Arc::new(RecordingBackend::new(mem));
    let log = rec.log();
    let backbone: Arc<dyn Backend> = rec;
    (backbone, log)
}

/// One transport per client over `backbone`. With faults enabled each is an
/// active, tape-guided [`FaultBackend`]; otherwise each client shares the
/// backbone directly. Returns the transports (for the outage nemesis and final
/// healing) and the per-client backends, in client order.
fn build_transports(
    backbone: &Arc<dyn Backend>,
    faults: FaultConfig,
    seed: u64,
    streams: &[Vec<u8>; FAULT_STREAMS],
    nclients: usize,
) -> (Vec<Arc<FaultBackend>>, Vec<Arc<dyn Backend>>) {
    let mut transports: Vec<Arc<FaultBackend>> = Vec::new();
    let mut client_backends: Vec<Arc<dyn Backend>> = Vec::with_capacity(nclients);
    if faults.enabled {
        let fopts = FaultOptions::from_intensity(faults.intensity);
        for i in 0..nclients {
            let tape = streams[CLIENT_STREAM_BASE + i % MAX_CLIENTS].clone();
            let t = FaultBackend::with_tape(backbone.clone(), tape, client_seed(seed, i), fopts);
            t.set_active(true);
            transports.push(t.clone());
            client_backends.push(t);
        }
    } else {
        for _ in 0..nclients {
            client_backends.push(backbone.clone());
        }
    }
    (transports, client_backends)
}

/// Spawns the crash and outage nemeses when faults are enabled, each on its own
/// fault-tape stream and a distinct fallback seed. The caller spawns the client
/// tasks first, so the fixed spawn order (clients, then crash, then outage)
/// keeps task ids — and thus the schedule — deterministic. Returns their join
/// handles (both `None` with faults off).
#[allow(clippy::type_complexity)]
fn spawn_nemeses(
    faults: FaultConfig,
    seed: u64,
    streams: &[Vec<u8>; FAULT_STREAMS],
    signals: &[CancellationToken],
    transports: &[Arc<FaultBackend>],
) -> (Option<rt::JoinHandle<()>>, Option<rt::JoinHandle<()>>) {
    if !faults.enabled {
        return (None, None);
    }
    let crash_tape = Tape::new(streams[CRASH_STREAM].clone(), seed ^ 0x00C0_FFEE_C0DE_BEEF);
    let crash = rt::spawn(crash_nemesis(
        signals.to_vec(),
        faults.intensity,
        crash_tape,
    ));
    let outage_tape = Tape::new(streams[OUTAGE_STREAM].clone(), seed ^ 0xFEED_FACE_DEAD_5EED);
    let outage = rt::spawn(outage_nemesis(
        transports.to_vec(),
        faults.intensity,
        outage_tape,
    ));
    (Some(crash), Some(outage))
}

/// Core harness: seed the store, run the clients as interleaved tasks under the
/// (optional) fault nemesis, then verify the per-key bound. Always records the
/// backend op stream and returns it for byte-for-byte determinism comparison.
async fn run_inner(
    workload: Workload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    // The fault tape guides each client's transport faults, crash timing, and
    // outage windows; with an empty tape all fall back to the seed
    // (PCT/seed-breadth runs).
    let streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

    // The store and a shared recorder form a faultless backbone; each client gets
    // its own transport (`FaultBackend`) over it.
    let (backbone, log) = make_backbone();

    // Initialize the collection and seed every key to zero up front (over the
    // faultless backbone, so this cannot fail spuriously).
    let init_db = open_det_db(&backbone).await.expect("open init db");
    let init_coll = init_db.collection(COLLECTION);
    init_coll.create().await.expect("create collection");
    let seed_coll = &init_coll;
    init_db
        .tx(|tx| async move {
            for k in 0..KEY_COUNT {
                tx.write(seed_coll, &key_name(k), &write_int(0))?;
            }
            Ok(())
        })
        .await
        .expect("seed keys");

    let expected = expected_increments(&workload);
    let acct = Arc::new(Mutex::new(Acct::new()));

    // One transport per client over the shared backbone. Faults are live only
    // while the clients run.
    let nclients = workload.clients.len();
    let (transports, client_backends) =
        build_transports(&backbone, faults, seed, &streams, nclients);

    // Each client runs as its own task over its own transport so the scheduler
    // can interleave them. A `CancellationToken` lets the crash nemesis
    // simulate a hard crash by racing the signal against the client's run
    // loop; the dropped future is the in-Rust analog of a process death. On
    // a clean run we `Database::shutdown` to drain in-flight transactions and
    // dedup owners before the Database clone drops; on a crash we *skip* shutdown
    // and let `Drop` tear everything down abruptly — that is the whole point
    // of the crash nemesis. The background loops are torn down in both cases
    // by `Background::drop` once the last `Database` clone goes out of scope (the
    // captured-task cycle is broken by subsystems holding `Weak<Background>`).
    let mut handles = Vec::with_capacity(nclients);
    let mut signals: Vec<CancellationToken> = Vec::with_capacity(nclients);
    for (ops, backend) in workload.clients.into_iter().zip(client_backends) {
        let signal = CancellationToken::new();
        signals.push(signal.clone());
        let acct = acct.clone();
        handles.push(rt::spawn(async move {
            let consumed = Arc::new(AtomicUsize::new(0));
            let crashed = {
                let Ok(db) = open_det_db(&backend).await else {
                    return;
                };
                let coll = db.collection(COLLECTION);
                let crashed = tokio::select! {
                    biased;
                    _ = signal.cancelled() => true,
                    _ = run_client(&db, &coll, &ops, &acct, &consumed) => false,
                };
                if !crashed {
                    db.shutdown().await;
                }
                crashed
            };
            // Crash-and-restart: a cancelled (crashed) client reopens the Database on
            // the same backend and finishes its remaining ops, recovering its
            // own orphaned locks via lease expiry. The in-doubt op it died on
            // is left for recovery rather than replayed (which would
            // double-apply a non-idempotent RMW). The restart is uncancellable
            // so it runs to completion.
            let n = consumed.load(Ordering::SeqCst);
            if crashed && n < ops.len() {
                let dummy = AtomicUsize::new(0);
                open_and_run(&backend, &ops[n..], &acct, &dummy).await;
            }
        }));
    }

    // The crash and outage nemeses run concurrently with the clients, each on its
    // own slice of the fault tape (and a distinct fallback seed).
    let (crash, outage) = spawn_nemeses(faults, seed, &streams, &signals, &transports);

    for h in handles {
        let _ = h.await;
    }
    if let Some(h) = crash {
        let _ = h.await;
    }
    if let Some(h) = outage {
        let _ = h.await;
    }

    // Heal every transport before verifying so recovery reads cannot themselves
    // fail.
    for t in &transports {
        t.set_active(false);
    }

    // Read every key's final value (driving recovery of any crashed client's
    // locks via lease expiry) and check the invariant.
    let mut finals = vec![0i64; KEY_COUNT];
    for (k, slot) in finals.iter_mut().enumerate() {
        *slot = read_int(&init_coll.read(&key_name(k)).await.expect("final read"));
    }
    init_db.shutdown().await;

    let acct = acct.lock().unwrap();
    assert_bounds(&acct, &finals, &expected, faults.enabled);
    log
}

// ---------------------------------------------------------------------------
// Public entry points. These are plain async fns; the deterministic driver
// (a `TapeScheduler`/seed under `rt::block_on_with`) is supplied by the fuzz
// target and the `concurrent_sim` self-check.
// ---------------------------------------------------------------------------

/// Runs `workload` over a fresh in-memory store and asserts serializability,
/// without injecting faults.
pub async fn run_and_assert(workload: Workload) {
    run_inner(workload, FaultConfig::none(), 0, Vec::new()).await;
}

/// Like [`run_and_assert`] but injects backend faults and client crashes per
/// `faults`. `fault_tape` guides the fault schedule (the fuzzer's secondary
/// tape); once it is exhausted, decisions fall back to `seed`.
pub async fn run_and_assert_with_faults(
    workload: Workload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) {
    run_inner(workload, faults, seed, fault_tape).await;
}

/// Like [`run_and_assert`] but records the ordered stream of backend operations
/// and returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_and_record(workload: &Workload) -> OpLog {
    run_inner(workload.clone(), FaultConfig::none(), 0, Vec::new()).await
}

/// Like [`run_and_record`] but with fault injection enabled per `faults`.
/// `fault_tape` guides the fault schedule; it falls back to `seed` once spent.
pub async fn run_and_record_with_faults(
    workload: &Workload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    run_inner(workload.clone(), faults, seed, fault_tape).await
}

// ---------------------------------------------------------------------------
// PCT seed-breadth run mode (ADR-011). Only under `--cfg sim`: these drive the
// harness on the deterministic executor with a `PctScheduler` instead of a
// fuzzer tape, so they complement (rather than replace) the coverage-guided
// `fuzz/` target. Each run is a pure function of `seed`, so a failure reproduces
// exactly by re-running that seed.
// ---------------------------------------------------------------------------

/// Default bug depth the PCT scheduler targets (preemption points + 1).
#[cfg(sim)]
pub const PCT_DEFAULT_DEPTH: usize = 3;

/// Rough estimate of the scheduling steps a workload run makes; affects only the
/// distribution of PCT change points, not correctness.
#[cfg(sim)]
pub const PCT_DEFAULT_STEPS: u64 = 2048;

#[cfg(sim)]
struct DecodedFuzzInput<W> {
    seed: u64,
    workload: W,
    faults: FaultConfig,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
}

#[cfg(sim)]
fn decode_fuzz_input<W>(data: &[u8]) -> DecodedFuzzInput<W>
where
    W: for<'a> Arbitrary<'a> + Default,
{
    let mut u = Unstructured::new(data);
    let seed: u64 = u.arbitrary().unwrap_or(0);
    let workload = W::arbitrary(&mut u).unwrap_or_default();
    let faults = FaultConfig::arbitrary(&mut u).unwrap_or_default();
    // Split the remaining bytes into a schedule tape and a fault tape; the
    // scheduler and fault schedule both fall back to defaults once spent.
    let rest = u.take_rest();
    let mid = rest.len() / 2;
    DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape: rest[..mid].to_vec(),
        fault_tape: rest[mid..].to_vec(),
    }
}

/// Decodes one libFuzzer input exactly as the `concurrent_tx` target does and
/// runs it on the deterministic executor, asserting the bound. Panics on any
/// violation. Shared by the fuzz target and the corpus-replay test so the two
/// can never diverge.
#[cfg(sim)]
pub fn replay_fuzz_input(data: &[u8]) {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<Workload>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        run_and_assert_with_faults(workload, faults, seed, fault_tape).await
    });
}

/// Decodes one libFuzzer input exactly as [`replay_fuzz_input`] does, runs it,
/// and returns the recorded backend op stream. Used by corpus replay tests to
/// prove committed inputs replay byte-for-byte, not just invariant-cleanly.
#[cfg(sim)]
pub fn record_fuzz_input(data: &[u8]) -> Vec<OpRecord> {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<Workload>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        let log = run_and_record_with_faults(&workload, faults, seed, fault_tape).await;
        let recorded = log.lock().unwrap();
        recorded.clone()
    })
}

/// Runs `workload` once under a PCT schedule seeded by `seed`, asserting the
/// serializability bound. Panics on any violation.
#[cfg(sim)]
pub fn pct_assert(workload: &Workload, faults: FaultConfig, seed: u64) {
    let w = workload.clone();
    rt::block_on_with(
        rt::PctScheduler::new(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS),
        seed,
        // Empty fault tape: PCT explores the seed-breadth fault space.
        async move { run_and_assert_with_faults(w, faults, seed, Vec::new()).await },
    );
}

/// Runs `workload` under a PCT schedule and returns the recorded backend op
/// stream, for per-seed determinism comparison.
#[cfg(sim)]
pub fn pct_record(workload: &Workload, faults: FaultConfig, seed: u64) -> OpLog {
    let w = workload.clone();
    rt::block_on_with(
        rt::PctScheduler::new(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS),
        seed,
        async move { run_and_record_with_faults(&w, faults, seed, Vec::new()).await },
    )
}

/// Seed-breadth sweep: runs `workload` under one PCT schedule per seed, asserting
/// the invariant on each. This is the seed-loop entry that complements the
/// coverage-guided tape fuzzer.
#[cfg(sim)]
pub fn pct_sweep(workload: &Workload, faults: FaultConfig, seeds: impl IntoIterator<Item = u64>) {
    for seed in seeds {
        pct_assert(workload, faults, seed);
    }
}

// ===========================================================================
// Cycle workload (ported from FoundationDB's `Cycle.cpp`).
//
// Keys form a ring `key(i) -> (i + 1) % N`; each transaction reads three
// consecutive next-pointers and rotates their edges, which preserves a single
// N-cycle. Because the rotation does not commute, any isolation or atomicity
// break splits, shrinks, or grows the ring — a true serializability oracle that
// the commutative RMW-increment workload above cannot provide. The ring stays
// valid whether a swap commits or aborts, so the invariant holds as-is under the
// crash/outage/lost-ack nemeses, with no relaxation.
//
// Alongside the swaps, an optional read-only observer snapshots the whole ring
// in one transaction (reading all N pointers concurrently) and asserts that
// snapshot is itself a valid ring. That adds a read-side serializability oracle
// — a committed read-only tx must observe a single committed state — and is the
// only workload that exercises `Transaction`'s concurrent-read path.
// ===========================================================================

/// Smallest ring size that guarantees the four nodes a swap touches (the start
/// node and its next three along the ring) are distinct, so the rotation
/// preserves a single cycle.
const MIN_NODES: usize = 4;
/// Largest ring the workload generates (keeps state small for the executor).
const MAX_NODES: usize = 12;
/// Most snapshots the concurrent read-only observer takes during a run (keeps
/// its work bounded at `MAX_SNAPSHOTS * node_count` reads).
const MAX_SNAPSHOTS: usize = MAX_OPS_PER_CLIENT;
/// Collection the ring lives in.
const CYCLE_COLLECTION: &[u8] = b"cycle";

/// A Cycle workload: `node_count` keys arranged in a ring, plus one swap
/// sequence per client. Each `usize` is a swap's start node, already reduced to
/// `0..node_count`. Clients run concurrently.
#[derive(Debug, Clone)]
pub struct CycleWorkload {
    /// Number of nodes in the ring (>= [`MIN_NODES`]).
    pub node_count: usize,
    /// Per-client swap start nodes.
    pub clients: Vec<Vec<usize>>,
    /// How many times a concurrent read-only observer snapshots the whole ring
    /// while the swaps run (`0` disables it). Each snapshot reads all `N`
    /// next-pointers concurrently in one transaction and must observe a valid
    /// ring — a read-side serializability oracle (see [`read_ring_snapshot`]).
    pub snapshot_reads: usize,
}

impl Default for CycleWorkload {
    fn default() -> Self {
        CycleWorkload {
            node_count: MIN_NODES,
            clients: Vec::new(),
            snapshot_reads: 0,
        }
    }
}

impl<'a> Arbitrary<'a> for CycleWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let node_count = MIN_NODES + (u.arbitrary::<u8>()? as usize % (MAX_NODES - MIN_NODES + 1));
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for _ in 0..nclients {
            let nswaps = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut swaps = Vec::with_capacity(nswaps);
            for _ in 0..nswaps {
                swaps.push(u.arbitrary::<u8>()? as usize % node_count);
            }
            clients.push(swaps);
        }
        let snapshot_reads = u.arbitrary::<u8>()? as usize % (MAX_SNAPSHOTS + 1);
        Ok(CycleWorkload {
            node_count,
            clients,
            snapshot_reads,
        })
    }
}

/// Reads node `idx`'s next-pointer within `tx`.
async fn read_next(tx: &crate::Transaction, coll: &Collection, idx: usize) -> Result<usize, Error> {
    let v = tx.read(coll, &key_name(idx)).await?;
    Ok(read_int(&v) as usize)
}

/// Sets node `idx`'s next-pointer to `next` within `tx`.
fn write_next(
    tx: &crate::Transaction,
    coll: &Collection,
    idx: usize,
    next: usize,
) -> Result<(), Error> {
    tx.write(coll, &key_name(idx), &write_int(next as i64))
}

/// Asserts the stored next-pointers still form a single ring over every node in
/// `0..N`: walking from node 0 visits each node exactly once and returns to 0
/// after exactly `N` hops. A serializability or atomicity failure splits the
/// ring (a revisit / early return to 0), drops a node, or dangles a pointer out
/// of range.
fn assert_ring(next: &[usize]) {
    let n = next.len();
    let mut seen = vec![false; n];
    let mut cur = 0usize;
    for hop in 0..n {
        assert!(
            cur < n,
            "ring pointer {cur} out of range (n={n}) at hop {hop}"
        );
        assert!(
            !seen[cur],
            "ring revisits node {cur} at hop {hop}: cycle shorter than {n}"
        );
        seen[cur] = true;
        cur = next[cur];
    }
    assert_eq!(
        cur, 0,
        "ring does not return to node 0 after {n} hops (broken or longer cycle)"
    );
}

/// One ring edge-rotation transaction starting at node `r` (mirrors FDB's
/// Cycle). Reads three consecutive next-pointers, then rotates the edges. For a
/// single N-cycle with N >= 4 the four nodes are distinct, so the three writes
/// target distinct keys and map the ring to another single N-cycle.
async fn cycle_swap(db: &Database, coll: &Collection, r: usize) -> Result<(), Error> {
    db.tx(|tx| async move {
        let r2 = read_next(&tx, coll, r).await?;
        let r3 = read_next(&tx, coll, r2).await?;
        let r4 = read_next(&tx, coll, r3).await?;
        write_next(&tx, coll, r, r3)?;
        write_next(&tx, coll, r2, r4)?;
        write_next(&tx, coll, r3, r2)
    })
    .await
}

/// Snapshots the whole ring within a single transaction, reading all `N`
/// next-pointers *concurrently* (joined together as in-flight backend fetches),
/// and returns the snapshot. Unlike [`cycle_swap`]'s dependent pointer walk,
/// these reads have no data dependency, so this is what exercises `Transaction`'s
/// concurrent-read path (see `tx.rs`: `read` takes `&self` and never holds the
/// lock across `.await`). A committed read-only transaction observes exactly one
/// committed state under serializable isolation, so the caller can assert the
/// snapshot is itself a valid ring.
async fn read_ring_snapshot(
    db: &Database,
    coll: &Collection,
    node_count: usize,
) -> Result<Vec<usize>, Error> {
    db.tx(|tx| async move {
        futures::future::try_join_all((0..node_count).map(|k| read_next(&tx, coll, k))).await
    })
    .await
}

/// Runs a client's swaps in order. Returns the number consumed: `swaps.len()` if
/// all succeeded, or `i + 1` if swap `i` failed (left for recovery rather than
/// replayed on restart).
async fn run_cycle_client(
    db: &Database,
    coll: &Collection,
    swaps: &[usize],
    consumed: &AtomicUsize,
) -> usize {
    for (i, r) in swaps.iter().enumerate() {
        consumed.store(i + 1, Ordering::SeqCst);
        if cycle_swap(db, coll, *r).await.is_err() {
            return i + 1;
        }
    }
    consumed.store(swaps.len(), Ordering::SeqCst);
    swaps.len()
}

/// Opens the Database on `backend`, runs `swaps`, and closes it. Records swaps
/// consumed in `consumed` (see [`run_cycle_client`]); leaves it at `0` if the
/// open itself failed.
async fn open_and_run_cycle(backend: &Arc<dyn Backend>, swaps: &[usize], consumed: &AtomicUsize) {
    let Ok(db) = open_det_db(backend).await else {
        return;
    };
    let coll = db.collection(CYCLE_COLLECTION);
    let _ = run_cycle_client(&db, &coll, swaps, consumed).await;
    db.shutdown().await;
}

/// Core Cycle harness: lay down the ring, run the clients as interleaved tasks
/// under the (optional) fault nemesis, then assert the ring invariant. Mirrors
/// [`run_inner`], reusing the same backbone, transports, and nemeses. Always
/// records the backend op stream and returns it for determinism comparison.
async fn run_cycle_inner(
    workload: CycleWorkload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    let node_count = workload.node_count;
    let snapshot_reads = workload.snapshot_reads;
    let streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

    let (backbone, log) = make_backbone();

    // Initialize the collection and lay down the ring (key(i) -> (i + 1) % N)
    // over the faultless backbone, so setup cannot fail spuriously.
    let init_db = open_det_db(&backbone).await.expect("open init db");
    let init_coll = init_db.collection(CYCLE_COLLECTION);
    init_coll.create().await.expect("create collection");
    let seed_coll = &init_coll;
    init_db
        .tx(|tx| async move {
            for k in 0..node_count {
                write_next(&tx, seed_coll, k, (k + 1) % node_count)?;
            }
            Ok(())
        })
        .await
        .expect("seed ring");

    // One transport per client over the shared backbone.
    let nclients = workload.clients.len();
    let (transports, client_backends) =
        build_transports(&backbone, faults, seed, &streams, nclients);

    // Each client runs its swaps as its own task so the scheduler interleaves
    // them; a crashed client restarts on the same backend to finish its
    // remaining swaps, recovering its own orphaned locks via lease expiry.
    // On a clean run the Database is gracefully shut down (drain in-flight + dedup
    // owners); on a crash we drop the Database without shutdown — that's the
    // in-Rust analog of process death and the whole point of the crash
    // nemesis. `Background::drop` still aborts spawned background loops in
    // both cases.
    let mut handles = Vec::with_capacity(nclients);
    let mut signals: Vec<CancellationToken> = Vec::with_capacity(nclients);
    for (swaps, backend) in workload.clients.into_iter().zip(client_backends) {
        let signal = CancellationToken::new();
        signals.push(signal.clone());
        handles.push(rt::spawn(async move {
            let consumed = Arc::new(AtomicUsize::new(0));
            let crashed = {
                let Ok(db) = open_det_db(&backend).await else {
                    return;
                };
                let coll = db.collection(CYCLE_COLLECTION);
                let crashed = tokio::select! {
                    biased;
                    _ = signal.cancelled() => true,
                    _ = run_cycle_client(&db, &coll, &swaps, &consumed) => false,
                };
                if !crashed {
                    db.shutdown().await;
                }
                crashed
            };
            let n = consumed.load(Ordering::SeqCst);
            if crashed && n < swaps.len() {
                let dummy = AtomicUsize::new(0);
                open_and_run_cycle(&backend, &swaps[n..], &dummy).await;
            }
        }));
    }

    // A concurrent read-only observer: while the swaps mutate the ring it
    // snapshots all N pointers in one transaction (concurrent reads, unlike the
    // swaps' dependent walk) and asserts the snapshot is itself a valid ring. A
    // committed read-only tx sees exactly one committed state under serializable
    // isolation, and every committed ring state is valid, so a torn snapshot that
    // still committed is a read-isolation bug `assert_ring` will catch. Runs on
    // the faultless backbone so it stays a reliable observer regardless of the
    // per-client transport faults. A panic here propagates out of the run, just
    // like the final verification.
    let reader = (snapshot_reads > 0).then(|| {
        let backbone = backbone.clone();
        rt::spawn(async move {
            let Ok(db) = open_det_db(&backbone).await else {
                return;
            };
            let coll = db.collection(CYCLE_COLLECTION);
            for _ in 0..snapshot_reads {
                rt::yield_now().await;
                match read_ring_snapshot(&db, &coll, node_count).await {
                    Ok(snap) => assert_ring(&snap),
                    Err(_) => break,
                }
            }
            db.shutdown().await;
        })
    });

    let (crash, outage) = spawn_nemeses(faults, seed, &streams, &signals, &transports);

    for h in handles {
        let _ = h.await;
    }
    if let Some(h) = reader {
        let _ = h.await;
    }
    if let Some(h) = crash {
        let _ = h.await;
    }
    if let Some(h) = outage {
        let _ = h.await;
    }

    // Heal every transport before verifying so recovery reads cannot fail.
    for t in &transports {
        t.set_active(false);
    }

    // Read every node's final next-pointer (driving recovery of any crashed
    // client's locks via lease expiry) and assert the ring is still intact.
    let mut next = vec![0usize; node_count];
    for (k, slot) in next.iter_mut().enumerate() {
        *slot = read_int(&init_coll.read(&key_name(k)).await.expect("final read")) as usize;
    }
    init_db.shutdown().await;

    assert_ring(&next);
    log
}

/// Runs a Cycle `workload` over a fresh in-memory ring and asserts the ring
/// invariant, without injecting faults.
pub async fn run_cycle_and_assert(workload: CycleWorkload) {
    run_cycle_inner(workload, FaultConfig::none(), 0, Vec::new()).await;
}

/// Like [`run_cycle_and_assert`] but injects backend faults and client crashes
/// per `faults`. The ring invariant holds regardless of how many swaps commit,
/// since each swap is atomic. `fault_tape` guides the fault schedule; it falls
/// back to `seed` once spent.
pub async fn run_cycle_and_assert_with_faults(
    workload: CycleWorkload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) {
    run_cycle_inner(workload, faults, seed, fault_tape).await;
}

/// Like [`run_cycle_and_assert`] but records the ordered backend op stream and
/// returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_cycle_and_record(workload: &CycleWorkload) -> OpLog {
    run_cycle_inner(workload.clone(), FaultConfig::none(), 0, Vec::new()).await
}

/// Like [`run_cycle_and_record`] but with fault injection enabled per `faults`.
pub async fn run_cycle_and_record_with_faults(
    workload: &CycleWorkload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    run_cycle_inner(workload.clone(), faults, seed, fault_tape).await
}

/// Decodes one libFuzzer input for the `cycle` target and runs it on the
/// deterministic executor, asserting the ring invariant. Panics on any
/// violation. Shared by the fuzz target and the corpus-replay test so the two
/// can never diverge.
#[cfg(sim)]
pub fn replay_cycle_input(data: &[u8]) {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<CycleWorkload>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        run_cycle_and_assert_with_faults(workload, faults, seed, fault_tape).await
    });
}

/// Decodes one Cycle libFuzzer input exactly as [`replay_cycle_input`] does,
/// runs it, and returns the recorded backend op stream for byte-identical
/// corpus replay checks.
#[cfg(sim)]
pub fn record_cycle_input(data: &[u8]) -> Vec<OpRecord> {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<CycleWorkload>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        let log = run_cycle_and_record_with_faults(&workload, faults, seed, fault_tape).await;
        let recorded = log.lock().unwrap();
        recorded.clone()
    })
}

/// Runs a Cycle `workload` once under a PCT schedule seeded by `seed`, asserting
/// the ring invariant. Panics on any violation.
#[cfg(sim)]
pub fn cycle_pct_assert(workload: &CycleWorkload, faults: FaultConfig, seed: u64) {
    let w = workload.clone();
    rt::block_on_with(
        rt::PctScheduler::new(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS),
        seed,
        async move { run_cycle_and_assert_with_faults(w, faults, seed, Vec::new()).await },
    );
}

/// Runs a Cycle `workload` under a PCT schedule and returns the recorded backend
/// op stream, for per-seed determinism comparison.
#[cfg(sim)]
pub fn cycle_pct_record(workload: &CycleWorkload, faults: FaultConfig, seed: u64) -> OpLog {
    let w = workload.clone();
    rt::block_on_with(
        rt::PctScheduler::new(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS),
        seed,
        async move { run_cycle_and_record_with_faults(&w, faults, seed, Vec::new()).await },
    )
}

/// Seed-breadth sweep over a Cycle `workload`: one PCT schedule per seed,
/// asserting the ring invariant on each.
#[cfg(sim)]
pub fn cycle_pct_sweep(
    workload: &CycleWorkload,
    faults: FaultConfig,
    seeds: impl IntoIterator<Item = u64>,
) {
    for seed in seeds {
        cycle_pct_assert(workload, faults, seed);
    }
}
