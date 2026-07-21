//! Shared deterministic execution, failure/delay injection, replay, and PCT harness.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
#[cfg(sim)]
use glassdb_backend::middleware::OpRecord;
use glassdb_backend::middleware::{FaultBackend, FaultOptions, OpLog, RecordingBackend};
use glassdb_concurr::{Tape, rt};
use glassdb_storage::SplitPolicy;
use tokio_util::sync::CancellationToken;

use crate::{Database, Error, ProtocolTiming};

use super::MAX_CLIENTS;
use super::slow_backend;

const DB_NAME: &str = "fuzz";
const SLOW_MUTATION_SEED: u64 = 0x510A_7E00_5EED_BA5E;

/// Controls transport failures, client crashes, and slow backend mutations in
/// the deterministic simulation harness.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultConfig {
    failures: bool,
    slow_mutations: bool,
    intensity: u8,
}

impl FaultConfig {
    /// Disables every injector.
    pub fn none() -> Self {
        Self::default()
    }

    /// Enables transport failures and client crashes at the given intensity.
    pub fn failures(intensity: u8) -> Self {
        FaultConfig {
            failures: true,
            slow_mutations: false,
            intensity,
        }
    }

    /// Enables one slow conditional mutation and no uncertain failures.
    pub fn slow_mutations() -> Self {
        FaultConfig {
            failures: false,
            slow_mutations: true,
            intensity: 0,
        }
    }

    /// Enables transport failures, client crashes, and one slow mutation.
    pub fn combined(intensity: u8) -> Self {
        FaultConfig {
            failures: true,
            slow_mutations: true,
            intensity,
        }
    }

    fn failures_enabled(self) -> bool {
        self.failures
    }

    fn slow_mutations_enabled(self) -> bool {
        self.slow_mutations
    }
}

impl<'a> Arbitrary<'a> for FaultConfig {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let mode = u.arbitrary::<u8>()? % 4;
        let intensity = u.arbitrary()?;
        Ok(match mode {
            0 => FaultConfig::none(),
            1 => FaultConfig::failures(intensity),
            2 => FaultConfig::slow_mutations(),
            _ => FaultConfig::combined(intensity),
        })
    }
}
/// Opens the simulation Database with the deterministic clock the fuzzer relies
/// on for byte-identical replays, under the given split `policy`. Workloads open
/// their databases through this helper (see [`SimWorkload::open_db`]) so the
/// deterministic clock is applied uniformly regardless of the chosen policy.
pub(crate) async fn open_det_db(
    backend: &Arc<dyn Backend>,
    policy: SplitPolicy,
) -> Result<Database, Error> {
    Database::builder(DB_NAME, backend.clone())
        .deterministic_time(true)
        .split_policy(policy)
        .protocol_timing(ProtocolTiming::simulation())
        .open()
        .await
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

/// One transport per client over `backbone`. With transport failures enabled,
/// each is an active, tape-guided [`FaultBackend`]; otherwise each client shares
/// the backbone directly. Returns the transports (for the outage nemesis and
/// final healing) and the per-client backends, in client order.
fn build_transports(
    backbone: &Arc<dyn Backend>,
    faults: FaultConfig,
    seed: u64,
    streams: &[Vec<u8>; FAULT_STREAMS],
    nclients: usize,
) -> (Vec<Arc<FaultBackend>>, Vec<Arc<dyn Backend>>) {
    let mut transports: Vec<Arc<FaultBackend>> = Vec::new();
    let mut client_backends: Vec<Arc<dyn Backend>> = Vec::with_capacity(nclients);
    if faults.failures_enabled() {
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

/// Spawns the crash and outage nemeses when transport failures are enabled,
/// each on its own fault-tape stream and a distinct fallback seed. The caller
/// spawns the client tasks first, so the fixed spawn order (clients, then crash,
/// then outage) keeps task ids — and thus the schedule — deterministic. Returns
/// their join handles (both `None` without transport failures).
#[allow(clippy::type_complexity)]
fn spawn_nemeses(
    faults: FaultConfig,
    seed: u64,
    streams: &[Vec<u8>; FAULT_STREAMS],
    signals: &[CancellationToken],
    transports: &[Arc<FaultBackend>],
) -> (Option<rt::JoinHandle<()>>, Option<rt::JoinHandle<()>>) {
    if !faults.failures_enabled() {
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

// ===========================================================================
// SimWorkload: the shared harness abstraction.
//
// Every deterministic-simulation workload (increment RMW, cycle, membership, API) is
// the same run: seed a shared store, run each client's op sequence as its own
// interleaved task over its own fault transport, run the crash/outage nemeses,
// then read the final committed state and assert an invariant. Only a few points
// differ per workload — opening the database, the seed step, how one op runs, the
// invariant, and an optional concurrent observer — so those are the trait
// methods. Each workload owns its own collection(s) behind those methods, so the
// harness works purely with `Database` handles and `run_generic` owns everything
// else (the backbone, per-client transports, crash/restart, and nemeses).
// ===========================================================================

/// A deterministic-simulation workload the shared harness ([`run_generic`]) can
/// drive. Implementors supply only what varies between workloads; the backbone,
/// per-client transports, crash-and-restart client tasks, and fault nemeses are
/// all provided by the harness.
pub trait SimWorkload: Clone + Default + Send + Sync + 'static {
    /// A single client operation, run in its own transaction.
    type Op: Clone + Send + Sync + 'static;
    /// Shared oracle state, updated as ops run and checked in [`verify`]. Carries
    /// its own interior mutability (e.g. a `Mutex`); use `()` when no state is
    /// needed.
    ///
    /// [`verify`]: SimWorkload::verify
    type State: Send + Sync + 'static;

    /// This run's per-client op sequences. Clients run concurrently.
    fn clients(&self) -> &[Vec<Self::Op>];

    /// A fresh oracle state for one run.
    fn new_state(&self) -> Self::State;

    /// Opens a database for this workload over `backend`. The harness calls this
    /// for the seed/verify database and for every client (and restart), so the
    /// workload — not the harness — chooses the split soft-cap policy. The
    /// default uses production caps; override to exercise B-link splits with few
    /// keys. Implementations must go through [`open_det_db`] to keep the
    /// deterministic clock byte-identical replays rely on.
    fn open_db(backend: &Arc<dyn Backend>) -> impl Future<Output = Result<Database, Error>> + Send {
        open_det_db(backend, SplitPolicy::default())
    }

    /// Creates and seeds this workload's collection(s) before the clients start,
    /// over the faultless backbone (so setup cannot fail spuriously).
    fn seed(&self, db: &Database) -> impl Future<Output = ()> + Send;

    /// Runs one op in its own transaction, updating `state`. Returns the op's
    /// result so the client loop can stop (and leave it in-doubt) on failure.
    fn run_op(
        db: &Database,
        op: &Self::Op,
        state: &Self::State,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Reads the final committed state and asserts the workload invariant.
    /// Panics on any violation. `failures_enabled` selects the exact vs. relaxed
    /// (in-doubt-tolerant) form of the invariant; slow-only runs remain exact.
    fn verify(
        &self,
        db: &Database,
        state: &Self::State,
        failures_enabled: bool,
    ) -> impl Future<Output = ()> + Send;

    /// An optional concurrent read-only observer spawned alongside the clients
    /// (e.g. the Cycle ring snapshotter). Spawned in a fixed order — after the
    /// clients, before the nemeses — so task ids stay deterministic. Default:
    /// none.
    fn spawn_observer(
        &self,
        _backbone: &Arc<dyn Backend>,
        _state: &Arc<Self::State>,
    ) -> Option<rt::JoinHandle<()>> {
        None
    }
}

/// Runs a client's op sequence in order. Returns the number of ops *consumed*:
/// `ops.len()` if all succeeded, or `i + 1` if op `i` failed (that op is left
/// in-doubt and is *not* replayed on restart, since re-running a non-idempotent
/// op would double-apply).
async fn run_generic_client<W: SimWorkload>(
    db: &Database,
    ops: &[W::Op],
    state: &W::State,
    consumed: &AtomicUsize,
    faults: FaultConfig,
) -> usize {
    for (i, op) in ops.iter().enumerate() {
        // Bump consumed *before* attempting the op. If the outer crash future
        // drops us mid-op, this op is counted as consumed (left in doubt) and is
        // not replayed by the restart path. We need this counter on a shared
        // atomic because the `tokio::select!` cancel arm simply drops this future
        // and cannot read its return value.
        consumed.store(i + 1, Ordering::SeqCst);
        if let Err(error) = W::run_op(db, op, state).await {
            assert_admissible_client_error(faults, "running client operation", error);
            return i + 1;
        }
    }
    consumed.store(ops.len(), Ordering::SeqCst);
    ops.len()
}

fn assert_admissible_client_error(faults: FaultConfig, context: &str, error: Error) {
    if client_error_is_admissible(faults, &error) {
        return;
    }
    panic!("{context} returned unexpected error: {error} ({error:?})");
}

fn client_error_is_admissible(faults: FaultConfig, error: &Error) -> bool {
    faults.failures_enabled() && matches!(error, Error::InDoubt(_) | Error::Unavailable(_))
}

/// Core harness, generic over the workload: seed the store, run the clients as
/// interleaved tasks under the (optional) fault nemesis and observer, then let
/// the workload verify its invariant. Always records the backend op stream and
/// returns it for byte-for-byte determinism comparison.
async fn run_generic<W: SimWorkload>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    // The fault tape guides each client's transport failures, crash timing,
    // outage windows, and the independent one-shot slow mutation. With an empty
    // tape all decisions fall back to the seed (PCT/seed-breadth runs).
    let streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

    // The store and a shared recorder form a faultless backbone; each client gets
    // its own transport (`FaultBackend`) over it.
    let (backbone, log) = make_backbone();

    // Let the workload open and seed its collection(s), over the faultless
    // backbone so setup cannot fail spuriously.
    let init_db = W::open_db(&backbone).await.expect("open init db");
    workload.seed(&init_db).await;
    init_db.shutdown().await;
    drop(init_db);

    let state = Arc::new(workload.new_state());

    // One transport per client over the shared backbone. Injectors are live
    // only while the clients run.
    let client_ops: Vec<Vec<W::Op>> = workload.clients().to_vec();
    let nclients = client_ops.len();
    let client_backbone: Arc<dyn Backend> = if faults.slow_mutations_enabled() {
        slow_backend::with_tape(
            backbone.clone(),
            fault_tape,
            seed ^ SLOW_MUTATION_SEED,
            ProtocolTiming::simulation(),
        )
    } else {
        backbone.clone()
    };
    let (transports, client_backends) =
        build_transports(&client_backbone, faults, seed, &streams, nclients);

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
    for (ops, backend) in client_ops.into_iter().zip(client_backends) {
        let signal = CancellationToken::new();
        signals.push(signal.clone());
        let state = state.clone();
        handles.push(rt::spawn(async move {
            let consumed = Arc::new(AtomicUsize::new(0));
            let crashed = {
                let db = match W::open_db(&backend).await {
                    Ok(db) => db,
                    Err(error) => {
                        assert_admissible_client_error(faults, "opening client database", error);
                        return;
                    }
                };
                let crashed = tokio::select! {
                    biased;
                    _ = signal.cancelled() => true,
                    _ = run_generic_client::<W>(&db, &ops, &state, &consumed, faults) => false,
                };
                if !crashed {
                    db.shutdown().await;
                }
                crashed
            };
            // Crash-and-restart: a cancelled (crashed) client reopens the Database
            // on the same backend and finishes its remaining ops, recovering its
            // own orphaned locks via lease expiry. The in-doubt op it died on is
            // left for recovery rather than replayed (which would double-apply a
            // non-idempotent op). The restart is uncancellable so it runs to
            // completion.
            let n = consumed.load(Ordering::SeqCst);
            if crashed && n < ops.len() {
                match W::open_db(&backend).await {
                    Ok(db) => {
                        let dummy = AtomicUsize::new(0);
                        run_generic_client::<W>(&db, &ops[n..], &state, &dummy, faults).await;
                        db.shutdown().await;
                    }
                    Err(error) => assert_admissible_client_error(
                        faults,
                        "reopening crashed client database",
                        error,
                    ),
                }
            }
        }));
    }

    // An optional concurrent observer, then the crash and outage nemeses, each on
    // its own slice of the fault tape (and a distinct fallback seed). The fixed
    // spawn order (clients, observer, crash, outage) keeps task ids — and thus
    // the schedule — deterministic.
    let observer = workload.spawn_observer(&backbone, &state);
    let (crash, outage) = spawn_nemeses(faults, seed, &streams, &signals, &transports);

    for h in handles {
        h.await.expect("client task failed");
    }
    if let Some(h) = observer {
        h.await.expect("observer task failed");
    }
    if let Some(h) = crash {
        h.await.expect("crash nemesis task failed");
    }
    if let Some(h) = outage {
        h.await.expect("outage nemesis task failed");
    }

    // Heal every transport before verifying so recovery reads cannot themselves
    // fail.
    for t in &transports {
        t.set_active(false);
    }

    // The workload reads the final committed state (driving recovery of any
    // crashed client's locks via lease expiry) and asserts its invariant.
    let verify_db = W::open_db(&backbone)
        .await
        .expect("open fresh verification db");
    workload
        .verify(&verify_db, &state, faults.failures_enabled())
        .await;
    verify_db.shutdown().await;
    log
}

// ---------------------------------------------------------------------------
// Public entry points, generic over the workload. These are plain async fns; the
// deterministic driver (a `TapeScheduler`/seed under `rt::block_on_with`) is
// supplied by the fuzz target and the `*_sim` self-checks.
// ---------------------------------------------------------------------------

/// Runs `workload` over a fresh in-memory store and asserts its invariant,
/// without injecting faults.
pub async fn run_and_assert<W: SimWorkload>(workload: W) {
    run_generic(workload, FaultConfig::none(), 0, Vec::new()).await;
}

/// Like [`run_and_assert`] but applies the failure and slow-mutation modes in
/// `faults`. `fault_tape` guides their schedule (the fuzzer's secondary tape);
/// once it is exhausted, decisions fall back to `seed`.
pub async fn run_and_assert_with_faults<W: SimWorkload>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) {
    run_generic(workload, faults, seed, fault_tape).await;
}

/// Like [`run_and_assert`] but records the ordered stream of backend operations
/// and returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_and_record<W: SimWorkload>(workload: &W) -> OpLog {
    run_generic(workload.clone(), FaultConfig::none(), 0, Vec::new()).await
}

/// Like [`run_and_record`] but with the configured failure/slow-mutation modes.
/// `fault_tape` guides injection; it falls back to `seed` once spent.
pub async fn run_and_record_with_faults<W: SimWorkload>(
    workload: &W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    run_generic(workload.clone(), faults, seed, fault_tape).await
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

/// Decodes one libFuzzer input for workload `W` exactly as its target does and
/// runs it on the deterministic executor, asserting the invariant. Panics on any
/// violation. Shared by the fuzz target and the corpus-replay test so the two
/// can never diverge.
#[cfg(sim)]
pub fn replay_input<W: SimWorkload + for<'a> Arbitrary<'a>>(data: &[u8]) {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<W>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        run_and_assert_with_faults(workload, faults, seed, fault_tape).await
    });
}

/// Decodes one libFuzzer input exactly as [`replay_input`] does, runs it, and
/// returns the recorded backend op stream. Used by corpus replay tests to prove
/// committed inputs replay byte-for-byte, not just invariant-cleanly.
#[cfg(sim)]
pub fn record_input<W: SimWorkload + for<'a> Arbitrary<'a>>(data: &[u8]) -> Vec<OpRecord> {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
    } = decode_fuzz_input::<W>(data);
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        let log = run_and_record_with_faults(&workload, faults, seed, fault_tape).await;
        let recorded = log.lock().unwrap();
        recorded.clone()
    })
}

/// Runs `workload` once under a PCT schedule seeded by `seed`, asserting its
/// invariant. Panics on any violation.
#[cfg(sim)]
pub fn pct_assert<W: SimWorkload>(workload: &W, faults: FaultConfig, seed: u64) {
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
pub fn pct_record<W: SimWorkload>(workload: &W, faults: FaultConfig, seed: u64) -> OpLog {
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
pub fn pct_sweep<W: SimWorkload>(
    workload: &W,
    faults: FaultConfig,
    seeds: impl IntoIterator<Item = u64>,
) {
    for seed in seeds {
        pct_assert(workload, faults, seed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_config_decodes_four_modes_without_shifting_the_tail() {
        let cases = [
            (0, false, false),
            (1, true, false),
            (2, false, true),
            (3, true, true),
        ];
        for (mode, failures, slow_mutations) in cases {
            let bytes = [mode, 99, 77];
            let mut input = Unstructured::new(&bytes);
            let decoded = FaultConfig::arbitrary(&mut input).unwrap();
            assert_eq!(decoded.failures, failures);
            assert_eq!(decoded.slow_mutations, slow_mutations);
            assert_eq!(input.len(), 1, "mode {mode} consumed the wrong byte count");
        }
    }

    #[test]
    fn only_uncertain_errors_are_admissible_with_failures() {
        let failures = FaultConfig::failures(1);
        assert!(client_error_is_admissible(
            failures,
            &Error::InDoubt("test".into())
        ));
        assert!(client_error_is_admissible(
            failures,
            &Error::Unavailable("test".into())
        ));
        assert!(!client_error_is_admissible(failures, &Error::NotFound));
        assert!(!client_error_is_admissible(
            FaultConfig::slow_mutations(),
            &Error::InDoubt("test".into())
        ));
    }

    #[derive(Clone, Default)]
    struct PanickingWorkload {
        clients: Vec<Vec<()>>,
    }

    impl SimWorkload for PanickingWorkload {
        type Op = ();
        type State = ();

        fn clients(&self) -> &[Vec<Self::Op>] {
            &self.clients
        }

        fn new_state(&self) -> Self::State {}

        async fn seed(&self, _db: &Database) {}

        async fn run_op(_db: &Database, _op: &Self::Op, _state: &Self::State) -> Result<(), Error> {
            panic!("intentional workload panic")
        }

        async fn verify(&self, _db: &Database, _state: &Self::State, _failures_enabled: bool) {}
    }

    #[tokio::test]
    #[should_panic(expected = "client task failed")]
    async fn client_task_panics_reach_the_harness() {
        run_and_assert(PanickingWorkload {
            clients: vec![vec![()]],
        })
        .await;
    }
}
