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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{FaultBackend, FaultOptions, OpLog, RecordingBackend};
use glassdb_backend::Backend;
use glassdb_concurr::{rt, CancelToken, Tape};

use crate::{Collection, Ctx, Error, Options, DB};

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

async fn read_int_from_tx(tx: &crate::Tx, c: &Collection, k: &[u8]) -> Result<i64, Error> {
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
    ctx: &Ctx,
    db: &DB,
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
    Ok(())
}

/// Runs a client's op sequence in order. Returns the number of ops *consumed*:
/// `ops.len()` if all succeeded, or `i + 1` if op `i` failed (that op is left
/// in-doubt and is *not* replayed on restart, since re-running a non-idempotent
/// RMW would double-apply).
async fn run_client(
    ctx: &Ctx,
    db: &DB,
    coll: &Collection,
    ops: &[Op],
    acct: &Mutex<Acct>,
) -> usize {
    for (i, op) in ops.iter().enumerate() {
        if run_one(ctx, db, coll, op, acct).await.is_err() {
            return i + 1;
        }
    }
    ops.len()
}

/// Opens the DB on `backend`, runs `ops`, and closes it. Returns ops consumed
/// (see [`run_client`]); `0` if the open itself failed (e.g. a crash during
/// startup).
async fn open_and_run(
    ctx: &Ctx,
    backend: &Arc<dyn Backend>,
    opts: &Options,
    ops: &[Op],
    acct: &Mutex<Acct>,
) -> usize {
    let Ok(db) = DB::open_with(ctx, DB_NAME, backend.clone(), opts.clone()).await else {
        return 0;
    };
    let coll = db.collection(COLLECTION);
    let consumed = run_client(ctx, &db, &coll, ops, acct).await;
    db.close().await;
    consumed
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

/// The crash nemesis: cancels a few clients' contexts at deterministic virtual
/// times, modelling an abrupt client stop mid-transaction. Each cancelled client
/// closes its `DB` (releasing background refreshers), so its in-flight locks are
/// recovered later via lease expiry during the final reads. Crash timing and
/// targets are drawn from `tape` (the fuzzer-guided fault schedule), falling back
/// to the tape's seeded PRNG once its bytes run out.
async fn crash_nemesis(tokens: Vec<CancelToken>, intensity: u8, mut tape: Tape) {
    let crashes = (intensity as usize % 3).min(tokens.len());
    for _ in 0..crashes {
        let gap = tape.below(40) + 1;
        rt::sleep(Duration::from_millis(gap)).await;
        let idx = tape.below(tokens.len() as u64) as usize;
        tokens[idx].cancel();
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

/// Core harness: seed the store, run the clients as interleaved tasks under the
/// (optional) fault nemesis, then verify the per-key bound. Always records the
/// backend op stream and returns it for byte-for-byte determinism comparison.
async fn run_inner(
    workload: Workload,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    let ctx = Ctx::background();
    let opts = deterministic_options();

    // The fault tape guides each client's transport faults, crash timing, and
    // outage windows; with an empty tape all fall back to the seed
    // (PCT/seed-breadth runs).
    let streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

    // The store and a shared recorder form a faultless backbone; each client gets
    // its own transport (`FaultBackend`) over it. Init and verification use the
    // backbone directly (a perfect connection).
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let rec = Arc::new(RecordingBackend::new(mem));
    let log = rec.log();
    let backbone: Arc<dyn Backend> = rec;

    // Initialize the collection and seed every key to zero up front (over the
    // faultless backbone, so this cannot fail spuriously).
    let init_db = DB::open_with(&ctx, DB_NAME, backbone.clone(), opts.clone())
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

    let expected = expected_increments(&workload);
    let acct = Arc::new(Mutex::new(Acct::new()));

    // One transport per client over the shared backbone. Faults are live only
    // while the clients run.
    let nclients = workload.clients.len();
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

    // Each client runs as its own task over its own transport so the scheduler
    // can interleave them.
    let mut handles = Vec::with_capacity(nclients);
    let mut tokens = Vec::with_capacity(nclients);
    for (ops, backend) in workload.clients.into_iter().zip(client_backends) {
        let (cctx, token) = Ctx::with_cancel();
        tokens.push(token);
        let opts = opts.clone();
        let acct = acct.clone();
        handles.push(rt::spawn(async move {
            let consumed = open_and_run(&cctx, &backend, &opts, &ops, &acct).await;
            // Crash-and-restart: a cancelled (crashed) client reopens the DB on
            // the same backend and finishes its remaining ops, recovering its own
            // orphaned locks via lease expiry. The in-doubt op it died on is left
            // for recovery rather than replayed (which would double-apply a
            // non-idempotent RMW). The restart is uncancellable so it runs to
            // completion.
            if consumed < ops.len() && cctx.is_cancelled() {
                let rctx = Ctx::background();
                let _ = open_and_run(&rctx, &backend, &opts, &ops[consumed..], &acct).await;
            }
        }));
    }

    // The crash and outage nemeses run concurrently with the clients, each on its
    // own slice of the fault tape (and a distinct fallback seed).
    let crash = if faults.enabled {
        let tape = Tape::new(streams[CRASH_STREAM].clone(), seed ^ 0x00C0_FFEE_C0DE_BEEF);
        Some(rt::spawn(crash_nemesis(
            tokens.clone(),
            faults.intensity,
            tape,
        )))
    } else {
        None
    };
    let outage = if faults.enabled {
        let tape = Tape::new(streams[OUTAGE_STREAM].clone(), seed ^ 0xFEED_FACE_DEAD_5EED);
        Some(rt::spawn(outage_nemesis(
            transports.clone(),
            faults.intensity,
            tape,
        )))
    } else {
        None
    };

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
        *slot = read_int(
            &init_coll
                .read_strong(&ctx, &key_name(k))
                .await
                .expect("final read"),
        );
    }
    init_db.close().await;

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
