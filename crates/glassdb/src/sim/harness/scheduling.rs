//! Schedule selection and deterministic executor entry points.

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::middleware::{OpLog, OpRecord};
use glassdb_concurr::rt;

use crate::sim::trace::{HarnessTrace, HarnessTraceEvent, TraceRunPhase, TraceSink};

use super::{
    FaultConfig, SimWorkload, deinterleave, run_and_assert_with_faults, run_and_record_with_faults,
    run_generic, run_generic_with_trace,
};

// PCT seed-breadth run mode (ADR-011) drives the harness with a PctScheduler
// instead of a fuzzer tape, complementing the coverage-guided fuzz target. Each
// run is a pure function of its seed, so failures reproduce by replaying it.

/// Default bug depth the PCT scheduler targets (preemption points + 1).
pub const PCT_DEFAULT_DEPTH: usize = 3;

/// Rough estimate of the scheduling steps a workload run makes; affects only the
/// distribution of PCT change points, not correctness.
pub const PCT_DEFAULT_STEPS: u64 = 2048;

struct DecodedFuzzInput<W> {
    seed: u64,
    workload: W,
    faults: FaultConfig,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
    media_tape: Vec<u8>,
}

fn decode_fuzz_input<W>(data: &[u8]) -> DecodedFuzzInput<W>
where
    W: for<'a> Arbitrary<'a> + Default,
{
    let mut u = Unstructured::new(data);
    let seed: u64 = u.arbitrary().unwrap_or(0);
    let workload = W::arbitrary(&mut u).unwrap_or_default();
    let faults = FaultConfig::arbitrary(&mut u).unwrap_or_default();
    // Each remaining byte guides exactly one of scheduling, backend faults, or
    // cache-media faults, keeping mutations local to one decision stream.
    let [schedule_tape, fault_tape, media_tape] = deinterleave::<3>(u.take_rest());
    DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
        media_tape,
    }
}

pub(super) fn run_fuzz_mode<W: SimWorkload>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
    media_tape: Option<Vec<u8>>,
) -> OpLog {
    rt::block_on_with(rt::TapeScheduler::new(schedule_tape), seed, async move {
        run_generic(workload, faults, seed, fault_tape, media_tape).await
    })
}

pub(super) fn run_fuzz_mode_traced<W: SimWorkload>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
    media_tape: Option<Vec<u8>>,
) -> (OpLog, HarnessTrace) {
    run_scheduled_mode_traced(
        workload,
        faults,
        seed,
        fault_tape,
        media_tape,
        move |trace| rt::TapeScheduler::new_traced(schedule_tape, trace),
    )
}

fn run_scheduled_mode_traced<W, S>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
    fault_tape: Vec<u8>,
    media_tape: Option<Vec<u8>>,
    scheduler: impl FnOnce(rt::RuntimeTraceObserver) -> S,
) -> (OpLog, HarnessTrace)
where
    W: SimWorkload,
    S: rt::Scheduler + 'static,
{
    let cached = media_tape.is_some();
    let trace = TraceSink::enabled();
    trace.record(HarnessTraceEvent::Run {
        cached,
        phase: TraceRunPhase::Started,
    });
    let runtime_trace = trace.runtime_observer();
    let run_trace = trace.clone();
    let scheduler = scheduler(runtime_trace.clone());
    let log = rt::block_on_with_trace(scheduler, seed, runtime_trace, async move {
        run_generic_with_trace(workload, faults, seed, fault_tape, media_tape, run_trace).await
    });
    trace.record(HarnessTraceEvent::Run {
        cached,
        phase: TraceRunPhase::Finished,
    });
    (log, trace.finish())
}

/// Decodes one libFuzzer input for workload `W` exactly as its target does and
/// runs it on fresh deterministic executors without and with the persistent
/// cache, asserting the invariant in both modes. The cached run injects only
/// basic media delays and pre-effect failures. Panics on any violation. Shared
/// by the fuzz target and the corpus-replay test so the two can never diverge.
pub fn replay_input<W: SimWorkload + for<'a> Arbitrary<'a>>(data: &[u8]) {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
        media_tape,
    } = decode_fuzz_input::<W>(data);
    run_fuzz_mode(
        workload.clone(),
        faults,
        seed,
        schedule_tape.clone(),
        fault_tape.clone(),
        None,
    );
    run_fuzz_mode(
        workload,
        faults,
        seed,
        schedule_tape,
        fault_tape,
        Some(media_tape),
    );
}

/// Decodes one libFuzzer input exactly as [`replay_input`] does, runs it, and
/// returns the cache-free and cache-enabled backend op streams concatenated.
/// Used by corpus replay tests to prove committed inputs replay byte-for-byte,
/// not just invariant-cleanly.
pub fn record_input<W: SimWorkload + for<'a> Arbitrary<'a>>(data: &[u8]) -> Vec<OpRecord> {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
        media_tape,
    } = decode_fuzz_input::<W>(data);
    let without_cache = run_fuzz_mode(
        workload.clone(),
        faults,
        seed,
        schedule_tape.clone(),
        fault_tape.clone(),
        None,
    );
    let with_cache = run_fuzz_mode(
        workload,
        faults,
        seed,
        schedule_tape,
        fault_tape,
        Some(media_tape),
    );
    let mut recorded = without_cache.lock().unwrap().clone();
    recorded.extend(with_cache.lock().unwrap().iter().cloned());
    recorded
}

/// Decodes and runs one fuzzer input like [`record_input`], returning the
/// structured cache-free and cache-enabled harness observations as one trace.
/// No digest or corpus baseline is imposed here; callers choose what to retain.
pub fn trace_input<W: SimWorkload + for<'a> Arbitrary<'a>>(data: &[u8]) -> HarnessTrace {
    let DecodedFuzzInput {
        seed,
        workload,
        faults,
        schedule_tape,
        fault_tape,
        media_tape,
    } = decode_fuzz_input::<W>(data);
    let (_, without_cache) = run_fuzz_mode_traced(
        workload.clone(),
        faults,
        seed,
        schedule_tape.clone(),
        fault_tape.clone(),
        None,
    );
    let (_, with_cache) = run_fuzz_mode_traced(
        workload,
        faults,
        seed,
        schedule_tape,
        fault_tape,
        Some(media_tape),
    );
    let mut events = without_cache.events;
    events.extend(with_cache.events);
    HarnessTrace { events }
}

/// Runs `workload` once under a PCT schedule seeded by `seed`, asserting its
/// invariant. Panics on any violation.
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
pub fn pct_record<W: SimWorkload>(workload: &W, faults: FaultConfig, seed: u64) -> OpLog {
    let w = workload.clone();
    rt::block_on_with(
        rt::PctScheduler::new(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS),
        seed,
        async move { run_and_record_with_faults(&w, faults, seed, Vec::new()).await },
    )
}

/// Runs `workload` under a traced PCT schedule without imposing a digest
/// baseline. The scheduler's change-point and spawn-priority draws are included.
pub fn pct_trace<W: SimWorkload>(workload: &W, faults: FaultConfig, seed: u64) -> HarnessTrace {
    run_pct_mode_traced(workload.clone(), faults, seed).1
}

pub(super) fn run_pct_mode_traced<W: SimWorkload>(
    workload: W,
    faults: FaultConfig,
    seed: u64,
) -> (OpLog, HarnessTrace) {
    run_scheduled_mode_traced(workload, faults, seed, Vec::new(), None, move |trace| {
        rt::PctScheduler::new_traced(seed, PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS, trace)
    })
}

/// Seed-breadth sweep: runs `workload` under one PCT schedule per seed, asserting
/// the invariant on each. This is the seed-loop entry that complements the
/// coverage-guided tape fuzzer.
pub fn pct_sweep<W: SimWorkload>(
    workload: &W,
    faults: FaultConfig,
    seeds: impl IntoIterator<Item = u64>,
) {
    for seed in seeds {
        pct_assert(workload, faults, seed);
    }
}
