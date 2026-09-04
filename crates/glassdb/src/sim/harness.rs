//! Shared deterministic execution, failure/delay injection, replay, and PCT harness.

mod client;
mod nemesis;
#[cfg(sim)]
mod scheduling;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{OpLog, RecordingBackend};
use glassdb_concurr::{Tape, rt};
use glassdb_storage::{InlinePolicy, SplitPolicy};
use tokio_util::sync::CancellationToken;

use crate::{Database, Error, PersistentCacheConfig, ProtocolTiming};

use self::client::ClientRunner;
use self::nemesis::{FaultTransports, NemesisRunner};
#[cfg(sim)]
pub use self::scheduling::{
    PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS, pct_assert, pct_record, pct_sweep, record_input,
    replay_input,
};
use super::slow_backend;
use super::{MAX_CLIENTS, MediaFaultProfile, SimMedia};

const DB_NAME: &str = "fuzz";
const SLOW_MUTATION_SEED: u64 = 0x510A_7E00_5EED_BA5E;
const CACHE_CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
const CACHE_MEDIA_SEED: u64 = 0xCA43_5EED_D15C_0048;

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
/// Opens a simulation database with the given split policy and optional
/// persistent-cache media.
pub(crate) async fn open_det_db(
    backend: &Arc<dyn Backend>,
    split_policy: SplitPolicy,
    inline_policy: InlinePolicy,
    media: Option<SimMedia>,
) -> Result<Database, Error> {
    let builder = Database::builder(DB_NAME, backend.clone())
        .split_policy(split_policy)
        .inline_policy(inline_policy)
        .protocol_timing(ProtocolTiming::simulation());
    let builder = if let Some(media) = media {
        builder.simulated_persistent_cache(
            PersistentCacheConfig {
                directory: PathBuf::from("simulated-fuzz-cache"),
                capacity_bytes: CACHE_CAPACITY_BYTES,
            },
            media,
        )
    } else {
        builder
    };
    builder.open().await
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

const INIT_VERIFY_MEDIA_STREAM: usize = 0;
const CLIENT_MEDIA_STREAM_BASE: usize = 1;
const OBSERVER_MEDIA_STREAM: usize = CLIENT_MEDIA_STREAM_BASE + MAX_CLIENTS;
const MEDIA_STREAMS: usize = OBSERVER_MEDIA_STREAM + 1;

struct RunMedia {
    init_and_verify: SimMedia,
    clients: Vec<SimMedia>,
    observer: SimMedia,
}

impl RunMedia {
    fn new(tape: Vec<u8>, seed: u64, client_count: usize) -> Self {
        assert!(client_count <= MAX_CLIENTS);
        let streams = deinterleave::<MEDIA_STREAMS>(&tape);
        let create = |stream: usize| {
            // Broad transaction workloads need ordinary latency and error
            // integration, while partial and indefinitely pending operations
            // remain in the isolated cache fault domain.
            SimMedia::new(
                MediaFaultProfile::Selected,
                streams[stream].clone(),
                seed ^ CACHE_MEDIA_SEED.wrapping_mul(stream as u64 + 1),
            )
        };
        Self {
            // Concurrent database handles need distinct exclusively opened
            // containers. Init and verification are sequential, so sharing
            // their medium also exercises clean reopen and timeline recovery.
            init_and_verify: create(INIT_VERIFY_MEDIA_STREAM),
            clients: (0..client_count)
                .map(|client| create(CLIENT_MEDIA_STREAM_BASE + client))
                .collect(),
            observer: create(OBSERVER_MEDIA_STREAM),
        }
    }
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

/// Spawns the crash and outage nemeses when transport failures are enabled,
/// each on its own fault-tape stream and a distinct fallback seed. The caller
/// spawns the client tasks and optional observer first, so the fixed spawn order
/// (clients, observer when enabled, crash, then outage) keeps task ids — and
/// thus the schedule — deterministic.
fn spawn_nemeses(
    faults: FaultConfig,
    seed: u64,
    streams: &[Vec<u8>; FAULT_STREAMS],
    signals: &[CancellationToken],
    transports: &FaultTransports,
) -> NemesisRunner {
    let mut nemeses = NemesisRunner::new();
    if !faults.failures_enabled() {
        return nemeses;
    }
    let crash_tape = Tape::new(streams[CRASH_STREAM].clone(), seed ^ 0x00C0_FFEE_C0DE_BEEF);
    nemeses.spawn_crash(signals, faults.intensity, crash_tape);
    let outage_tape = Tape::new(streams[OUTAGE_STREAM].clone(), seed ^ 0xFEED_FACE_DEAD_5EED);
    nemeses.spawn_outage(transports, faults.intensity, outage_tape);
    nemeses
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
// harness works purely with `Database` handles. The run context owns the
// backbone, media, model state, and per-client transports; `ClientRunner` owns
// client crash/restart lifecycles, and `NemesisRunner` owns the nemesis tasks.
// ===========================================================================

/// A deterministic-simulation workload the shared harness ([`run_generic`]) can
/// drive. Implementors supply only what varies between workloads; the backbone,
/// per-client transports, crash-and-restart client tasks, and fault nemeses are
/// all provided by the harness.
pub trait SimWorkload: Clone + Default + 'static {
    /// A single client operation, run in its own transaction.
    type Op: Clone + 'static;
    /// Shared oracle state, updated as ops run and checked in [`verify`]. Carries
    /// its own interior mutability (e.g. a `Mutex`); use `()` when no state is
    /// needed.
    ///
    /// [`verify`]: SimWorkload::verify
    type State: 'static;

    /// This run's per-client op sequences. Clients run concurrently.
    fn clients(&self) -> &[Vec<Self::Op>];

    /// A fresh oracle state for one run.
    fn new_state(&self) -> Self::State;

    /// Opens a database for this workload over `backend` and optional simulated
    /// cache media. The harness calls this for the seed/verify database and for
    /// every client (and restart), so the workload — not the harness — chooses
    /// the split soft-cap policy. The default uses production caps; override to
    /// exercise B-link splits with few keys. Implementations must go through
    /// [`open_det_db`] to preserve the deterministic clock required for
    /// byte-identical replay.
    fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> impl Future<Output = Result<Database, Error>> {
        open_det_db(
            backend,
            SplitPolicy::default(),
            InlinePolicy::default(),
            media,
        )
    }

    /// Creates and seeds this workload's collection(s) before the clients start,
    /// over the faultless backbone (so setup cannot fail spuriously).
    fn seed(&self, db: &Database) -> impl Future<Output = ()>;

    /// Runs one op in its own transaction, updating `state`. Returns the op's
    /// result so the client loop can stop (and leave it in-doubt) on failure.
    fn run_op(
        db: &Database,
        op: &Self::Op,
        state: &Self::State,
    ) -> impl Future<Output = Result<(), Error>>;

    /// Reads the final committed state and asserts the workload invariant.
    /// Panics on any violation. `failures_enabled` selects the exact vs. relaxed
    /// (in-doubt-tolerant) form of the invariant; slow-only runs remain exact.
    fn verify(
        &self,
        db: &Database,
        state: &Self::State,
        failures_enabled: bool,
    ) -> impl Future<Output = ()>;

    /// An optional concurrent read-only observer spawned alongside the clients
    /// (e.g. the Cycle ring snapshotter). Spawned in a fixed order — after the
    /// clients, before the nemeses — so task ids stay deterministic. Default:
    /// none.
    fn spawn_observer(
        &self,
        _backbone: &Arc<dyn Backend>,
        _state: &Arc<Self::State>,
        _media: Option<SimMedia>,
    ) -> Option<rt::JoinHandle<()>> {
        None
    }
}

fn client_error_is_admissible(faults: FaultConfig, error: &Error) -> bool {
    faults.failures_enabled() && matches!(error, Error::InDoubt(_) | Error::Unavailable(_))
}

/// Immutable inputs for one harness run.
struct RunPlan<W: SimWorkload> {
    workload: W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
    media_tape: Option<Vec<u8>>,
}

impl<W: SimWorkload> RunPlan<W> {
    fn new(
        workload: W,
        faults: FaultConfig,
        seed: u64,
        fault_tape: Vec<u8>,
        media_tape: Option<Vec<u8>>,
    ) -> Self {
        Self {
            workload,
            faults,
            seed,
            fault_tape,
            media_tape,
        }
    }

    async fn setup(self) -> RunContext<W> {
        let Self {
            workload,
            faults,
            seed,
            fault_tape,
            media_tape,
        } = self;

        // The fault tape guides each client's transport failures, crash timing,
        // outage windows, and the independent one-shot slow mutation. With an empty
        // tape all decisions fall back to the seed (PCT/seed-breadth runs).
        let fault_streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

        // The store and a shared recorder form a faultless backbone; each client gets
        // its own transport (`FaultBackend`) over it.
        let (backbone, log) = make_backbone();
        let client_ops: Vec<Vec<W::Op>> = workload.clients().to_vec();
        let nclients = client_ops.len();
        let run_media = media_tape.map(|tape| RunMedia::new(tape, seed, nclients));

        // Let the workload open and seed its collection(s), over the faultless
        // backbone so setup cannot fail spuriously.
        let init_db = W::open_db(
            &backbone,
            run_media
                .as_ref()
                .map(|media| media.init_and_verify.clone()),
        )
        .await
        .expect("open init db");
        workload.seed(&init_db).await;
        init_db.shutdown().await;
        drop(init_db);

        let state = Arc::new(workload.new_state());

        // One transport per client over the shared backbone. Injectors are live
        // only while the clients run.
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
        let transports = if faults.failures_enabled() {
            let schedules = (0..nclients)
                .map(|client| {
                    (
                        fault_streams[CLIENT_STREAM_BASE + client % MAX_CLIENTS].clone(),
                        client_seed(seed, client),
                    )
                })
                .collect();
            FaultTransports::faulting(&client_backbone, faults.intensity, schedules)
        } else {
            FaultTransports::faultless(&client_backbone, nclients)
        };

        RunContext {
            workload,
            faults,
            seed,
            fault_streams,
            backbone,
            log,
            client_ops,
            run_media,
            state,
            client_backbone,
            transports,
        }
    }
}

/// Resources whose lifetime spans one harness run.
struct RunContext<W: SimWorkload> {
    workload: W,
    faults: FaultConfig,
    seed: u64,
    fault_streams: [Vec<u8>; FAULT_STREAMS],
    backbone: Arc<dyn Backend>,
    log: OpLog,
    client_ops: Vec<Vec<W::Op>>,
    run_media: Option<RunMedia>,
    state: Arc<W::State>,
    client_backbone: Arc<dyn Backend>,
    transports: FaultTransports,
}

impl<W: SimWorkload> RunContext<W> {
    fn start_clients(&mut self) -> ClientRunner {
        ClientRunner::spawn::<W>(
            std::mem::take(&mut self.client_ops),
            self.transports.take_client_backends(),
            self.run_media.as_ref(),
            &self.state,
            self.faults,
        )
    }

    async fn teardown(self) -> OpLog {
        // Heal every transport before verifying so recovery reads cannot themselves
        // fail.
        self.transports.final_heal();

        // The workload reads the final committed state (driving recovery of any
        // crashed client's locks via lease expiry) and asserts its invariant.
        let verify_db = W::open_db(
            &self.backbone,
            self.run_media
                .as_ref()
                .map(|media| media.init_and_verify.clone()),
        )
        .await
        .expect("open fresh verification db");
        self.workload
            .verify(&verify_db, &self.state, self.faults.failures_enabled())
            .await;
        verify_db.shutdown().await;
        drop(self.client_backbone);
        self.log
    }
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
    media_tape: Option<Vec<u8>>,
) -> OpLog {
    let plan = RunPlan::new(workload, faults, seed, fault_tape, media_tape);
    let mut context = plan.setup().await;
    let mut clients = context.start_clients();

    // An optional concurrent observer, then the crash and outage nemeses, each on
    // its own slice of the fault tape (and a distinct fallback seed). The fixed
    // spawn order (clients, observer, crash, outage) keeps task ids — and thus
    // the schedule — deterministic.
    let observer = context.workload.spawn_observer(
        &context.backbone,
        &context.state,
        context
            .run_media
            .as_ref()
            .map(|media| media.observer.clone()),
    );
    let nemeses = spawn_nemeses(
        context.faults,
        context.seed,
        &context.fault_streams,
        clients.signals(),
        &context.transports,
    );

    clients.join().await;
    if let Some(h) = observer {
        h.await.expect("observer task failed");
    }
    nemeses.join().await;

    context.teardown().await
}

// ---------------------------------------------------------------------------
// Public entry points, generic over the workload. These are plain async fns; the
// deterministic driver (a `TapeScheduler`/seed under `exec::block_on_with`) is
// supplied by the fuzz target and the `*_sim` self-checks.
// ---------------------------------------------------------------------------

/// Runs `workload` over a fresh in-memory store and asserts its invariant,
/// without injecting faults.
pub async fn run_and_assert<W: SimWorkload>(workload: W) {
    run_generic(workload, FaultConfig::none(), 0, Vec::new(), None).await;
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
    run_generic(workload, faults, seed, fault_tape, None).await;
}

/// Like [`run_and_assert`] but records the ordered stream of backend operations
/// and returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_and_record<W: SimWorkload>(workload: &W) -> OpLog {
    run_generic(workload.clone(), FaultConfig::none(), 0, Vec::new(), None).await
}

/// Like [`run_and_record`] but with the configured failure/slow-mutation modes.
/// `fault_tape` guides injection; it falls back to `seed` once spent.
pub async fn run_and_record_with_faults<W: SimWorkload>(
    workload: &W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
) -> OpLog {
    run_generic(workload.clone(), faults, seed, fault_tape, None).await
}

#[cfg(test)]
mod sim_tests {
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

    #[test]
    #[should_panic(expected = "client task failed")]
    fn client_task_panics_reach_the_harness() {
        glassdb_concurr::exec::block_on(async {
            run_and_assert(PanickingWorkload {
                clients: vec![vec![()]],
            })
            .await;
        });
    }
}
