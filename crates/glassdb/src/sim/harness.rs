//! Shared deterministic execution, failure/delay injection, replay, and PCT harness.

mod client;
mod nemesis;
#[cfg(sim)]
mod scheduling;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{FaultOptions, OpLog, RecordingBackend};
use glassdb_concurr::{Tape, rt};
use glassdb_storage::{InlinePolicy, NodeSizePolicy};
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
use super::{CLIENT_COUNT, CLIENTS_PER_INSTANCE, MAX_INSTANCES, MediaFaultProfile, SimMedia};

const DB_NAME: &str = "fuzz";
const SLOW_MUTATION_SEED: u64 = 0x510A_7E00_5EED_BA5E;
const CACHE_CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
const CACHE_MEDIA_SEED: u64 = 0xCA43_5EED_D15C_0048;

/// Upper bound on the latency of each request and each reply without transport
/// failures. It stays far below the pending timeout of
/// [`ProtocolTiming::simulation`], so that latency alone does not expire the
/// leases of live transactions.
const FAULTLESS_MAX_DELAY: Duration = Duration::from_millis(10);

/// Controls transport failures, instance crashes, and slow backend mutations in
/// the deterministic simulation harness. The instance transports also add
/// latency without failures, so that the timers of background work elapse while
/// the clients run.
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultConfig {
    failures: bool,
    slow_mutations: bool,
    intensity: u8,
    zero_latency: bool,
}

impl FaultConfig {
    /// Disables every fault injector. The transports still add latency.
    pub fn none() -> Self {
        Self::default()
    }

    /// Enables transport failures and instance crashes at the given intensity.
    pub fn failures(intensity: u8) -> Self {
        FaultConfig {
            failures: true,
            intensity,
            ..Self::default()
        }
    }

    /// Enables one slow conditional mutation and no uncertain failures.
    pub fn slow_mutations() -> Self {
        FaultConfig {
            slow_mutations: true,
            ..Self::default()
        }
    }

    /// Enables transport failures, instance crashes, and one slow mutation.
    pub fn combined(intensity: u8) -> Self {
        FaultConfig {
            failures: true,
            slow_mutations: true,
            intensity,
            ..Self::default()
        }
    }

    fn failures_enabled(self) -> bool {
        self.failures
    }

    fn slow_mutations_enabled(self) -> bool {
        self.slow_mutations
    }

    /// The behavior of each instance transport while the clients run.
    fn transport_options(self) -> FaultOptions {
        if self.failures {
            return FaultOptions::from_intensity(self.intensity);
        }
        let delay_prob = if self.zero_latency { 0 } else { u8::MAX };
        FaultOptions {
            delay_prob,
            reply_delay_prob: delay_prob,
            fault_prob: 0,
            lost_ack_prob: 0,
            max_delay: FAULTLESS_MAX_DELAY,
        }
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
/// Opens a simulation database with the given node size policy and optional
/// persistent-cache media.
pub(crate) async fn open_det_db(
    backend: &Arc<dyn Backend>,
    node_size_policy: NodeSizePolicy,
    inline_policy: InlinePolicy,
    media: Option<SimMedia>,
) -> Result<Database, Error> {
    let builder = Database::builder(DB_NAME, backend.clone())
        .node_size_policy(node_size_policy)
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
const INSTANCE_MEDIA_STREAM_BASE: usize = 1;
const OBSERVER_MEDIA_STREAM: usize = INSTANCE_MEDIA_STREAM_BASE + MAX_INSTANCES;
const MEDIA_STREAMS: usize = OBSERVER_MEDIA_STREAM + 1;

struct RunMedia {
    init_and_verify: SimMedia,
    instances: Vec<SimMedia>,
    observer: SimMedia,
}

impl RunMedia {
    fn new(tape: Vec<u8>, seed: u64, instance_count: usize) -> Self {
        assert!(instance_count <= MAX_INSTANCES);
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
            // Independent database instances need distinct exclusively opened
            // containers. Init and verification are sequential, so sharing
            // their medium also exercises clean reopen and timeline recovery.
            init_and_verify: create(INIT_VERIFY_MEDIA_STREAM),
            instances: (0..instance_count)
                .map(|instance| create(INSTANCE_MEDIA_STREAM_BASE + instance))
                .collect(),
            observer: create(OBSERVER_MEDIA_STREAM),
        }
    }
}

// Fault-tape stream layout: one stream for each nemesis, plus one per instance
// transport (so each instance has its own fault decisions).
const CRASH_STREAM: usize = 0;
const OUTAGE_STREAM: usize = 1;
const INSTANCE_STREAM_BASE: usize = 2;
const FAULT_STREAMS: usize = INSTANCE_STREAM_BASE + MAX_INSTANCES;

/// Distinct PRNG-fallback seed for each instance transport.
fn instance_seed(seed: u64, i: usize) -> u64 {
    seed ^ 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i as u64 + 1)
}

/// The shared faultless backbone each database instance reaches through its
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
/// spawns the instance owners and optional observer first, so task creation
/// remains deterministic for a given schedule.
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
// Every deterministic-simulation workload follows the same run: seed a shared
// store, run each client's op sequence as its own task, sharing one database
// instance and fault transport per pair, run the crash/outage nemeses,
// then read the final committed state and assert an invariant. Only a few points
// differ per workload — opening the database, the seed step, how one op runs, the
// invariant, and an optional concurrent observer — so those are the trait
// methods. Each workload owns its own collection(s) behind those methods, so the
// harness works purely with `Database` handles. The run context owns the
// backbone, media, model state, and per-instance transports; `ClientRunner` owns
// instance crash/restart lifecycles, and `NemesisRunner` owns the nemesis tasks.
// ===========================================================================

/// A deterministic-simulation workload the shared harness ([`run_generic`]) can
/// drive. Implementors supply only what varies between workloads; the backbone,
/// per-instance transports, client tasks, instance restarts, and fault nemeses are
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

    /// Whether the oracle supports later operations after an admissible public
    /// error. The operation stays consumed and its recorded outcome is unchanged.
    const CONTINUE_AFTER_ADMISSIBLE_ERROR: bool = false;

    /// This run's per-client op sequences. Clients run concurrently.
    fn clients(&self) -> &[Vec<Self::Op>];

    /// A fresh oracle state for one run.
    fn new_state(&self) -> Self::State;

    /// Opens a database for this workload over `backend` and optional simulated
    /// cache media. The harness calls this for the seed/verify database and for
    /// every instance (and restart), so the workload — not the harness — chooses
    /// the node size policy. The default uses production caps; override to
    /// exercise B-link splits with few keys. Implementations must go through
    /// [`open_det_db`] to preserve the deterministic clock required for
    /// byte-identical replay.
    fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> impl Future<Output = Result<Database, Error>> {
        open_det_db(
            backend,
            NodeSizePolicy::default(),
            InlinePolicy::default(),
            media,
        )
    }

    /// Creates and seeds this workload's collection(s) before the clients start,
    /// over the faultless backbone (so setup cannot fail spuriously).
    fn seed(&self, db: &Database) -> impl Future<Output = ()>;

    /// Runs one op in its own transaction, updating `state`. Returns the op's
    /// public result so the client loop can apply its error policy. The workload
    /// must record that result before returning, including any unknown outcome.
    fn run_op(
        db: &Database,
        op: &Self::Op,
        state: &Self::State,
    ) -> impl Future<Output = Result<(), Error>>;

    /// Reads the final committed state and asserts the workload invariant.
    /// Panics on any violation. `allow_in_doubt` selects the form of the invariant
    /// that accepts interrupted operations, after faults or a foreground limit.
    /// Otherwise every planned operation must complete.
    fn verify(
        &self,
        db: &Database,
        state: &Self::State,
        allow_in_doubt: bool,
    ) -> impl Future<Output = ()>;

    /// An optional concurrent read-only observer spawned alongside the clients
    /// (e.g. the Cycle ring snapshotter). The harness spawns it after the instance
    /// owners and before the nemeses. The default has no observer.
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

        // The fault tape guides each instance's transport latency and failures,
        // crash timing, outage windows, and the independent one-shot slow
        // mutation. With an empty
        // tape all decisions fall back to the seed (PCT/seed-breadth runs).
        let fault_streams = deinterleave::<FAULT_STREAMS>(&fault_tape);

        // The store and a shared recorder form a faultless backbone; each instance gets
        // its own transport (`FaultBackend`) over it.
        let (backbone, log) = make_backbone();
        let client_ops: Vec<Vec<W::Op>> = workload.clients().to_vec();
        assert!(client_ops.len() <= CLIENT_COUNT);
        let ninstances = client_ops.len().div_ceil(CLIENTS_PER_INSTANCE);
        let run_media = media_tape.map(|tape| RunMedia::new(tape, seed, ninstances));

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

        // One transport per instance over the shared backbone. Injectors are live
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
        let schedules = (0..ninstances)
            .map(|instance| {
                (
                    fault_streams[INSTANCE_STREAM_BASE + instance].clone(),
                    instance_seed(seed, instance),
                )
            })
            .collect();
        let transports =
            FaultTransports::new(&client_backbone, faults.transport_options(), schedules);

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
            self.transports.take_instance_backends(),
            self.run_media.as_ref(),
            &self.state,
            self.faults,
        )
    }

    async fn teardown(self, timed_out: bool) -> OpLog {
        // Heal every transport before verifying so recovery reads cannot themselves
        // fail.
        self.transports.final_heal();

        // The workload reads the final committed state (driving recovery of any
        // crashed instance's locks via lease expiry) and asserts its invariant.
        let verify_db = W::open_db(
            &self.backbone,
            self.run_media
                .as_ref()
                .map(|media| media.init_and_verify.clone()),
        )
        .await
        .expect("open fresh verification db");
        self.workload
            .verify(
                &verify_db,
                &self.state,
                self.faults.failures_enabled() || timed_out,
            )
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
    // spawn order (instance owners, observer, crash, outage) keeps task creation
    // deterministic for a given schedule.
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

    let timed_out = clients.join().await;
    if let Some(h) = observer {
        h.await.expect("observer task failed");
    }
    nemeses.join().await;

    context.teardown(timed_out).await
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
    use std::sync::Mutex;
    use tokio::sync::Barrier;

    #[derive(Clone, Default)]
    struct SharedInstanceWorkload {
        clients: Vec<Vec<usize>>,
    }

    struct SharedInstanceState {
        starting: Barrier,
        completed: Barrier,
        snapshots: Mutex<Vec<(usize, crate::Stats)>>,
    }

    impl SimWorkload for SharedInstanceWorkload {
        type Op = usize;
        type State = SharedInstanceState;

        fn clients(&self) -> &[Vec<Self::Op>] {
            &self.clients
        }

        fn new_state(&self) -> Self::State {
            SharedInstanceState {
                starting: Barrier::new(4),
                completed: Barrier::new(4),
                snapshots: Mutex::new(Vec::new()),
            }
        }

        async fn seed(&self, _db: &Database) {}

        async fn run_op(db: &Database, client: &usize, state: &Self::State) -> Result<(), Error> {
            state.starting.wait().await;
            let key = super::super::key_name(*client);
            let key = &key;
            db.tx(|tx| async move {
                let root = tx.root_collection();
                tx.write(&root, key, b"value")
            })
            .await?;
            state.completed.wait().await;
            state.snapshots.lock().unwrap().push((*client, db.stats()));
            Ok(())
        }

        async fn verify(&self, db: &Database, state: &Self::State, _failures_enabled: bool) {
            for client in 0..4 {
                assert_eq!(
                    db.root_collection()
                        .read(&super::super::key_name(client))
                        .await
                        .unwrap(),
                    Some(b"value".to_vec()),
                );
            }
            let snapshots = state.snapshots.lock().unwrap();
            assert_eq!(snapshots.len(), 4);
            for (client, stats) in snapshots.iter() {
                assert_eq!(
                    stats.transactions.completed, 2,
                    "client {client}: {stats:?}"
                );
                assert!(
                    stats.coordinator.submissions > stats.coordinator.rounds,
                    "client {client}: {stats:?}"
                );
            }
        }
    }

    #[test]
    fn two_clients_share_each_database_and_coordinate_concurrent_writes() {
        for media_tape in [None, Some(Vec::new())] {
            glassdb_concurr::exec::block_on_with(
                glassdb_concurr::exec::RandomScheduler::new(3),
                3,
                async move {
                    run_generic(
                        SharedInstanceWorkload {
                            clients: (0..4).map(|id| vec![id]).collect(),
                        },
                        // A coordinator round takes only the writes that arrive
                        // together, and random latency spreads them apart.
                        FaultConfig {
                            zero_latency: true,
                            ..FaultConfig::none()
                        },
                        1,
                        Vec::new(),
                        media_tape,
                    )
                    .await;
                },
            );
        }
    }

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
