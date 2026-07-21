use glassdb::middleware::{OpRecord, first_divergence};
use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{FaultConfig, SimWorkload, run_and_record, run_and_record_with_faults};

/// Returns a deterministic schedule tape long enough for a simulation run.
pub fn tape(seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..8192)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}

/// Returns a fault tape independent from the schedule tape for the same seed.
pub fn fault_tape(seed: u64) -> Vec<u8> {
    tape(seed ^ 0xA5A5_A5A5_A5A5_A5A5)
}

pub fn assert_no_divergence(label: &str, first: &[OpRecord], second: &[OpRecord]) {
    if let Some((index, first_op, second_op)) = first_divergence(first, second) {
        panic!(
            "{label}: op stream diverged at index {index}\n  \
             run 1 ({} ops): {first_op:?}\n  run 2 ({} ops): {second_op:?}",
            first.len(),
            second.len(),
        );
    }
}

pub fn record_once<W: SimWorkload>(seed: u64, workload: &W) -> Vec<OpRecord> {
    let workload = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_and_record(&workload).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

pub fn record_faults_with_tape<W: SimWorkload>(
    seed: u64,
    workload: &W,
    faults: FaultConfig,
    fault_tape: Vec<u8>,
) -> Vec<OpRecord> {
    record_with_tapes(seed, workload, faults, tape(seed), fault_tape)
}

pub fn record_with_tapes<W: SimWorkload>(
    seed: u64,
    workload: &W,
    faults: FaultConfig,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
) -> Vec<OpRecord> {
    let workload = workload.clone();
    let log = block_on_with(TapeScheduler::new(schedule_tape), seed, async move {
        run_and_record_with_faults(&workload, faults, seed, fault_tape).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

pub fn assert_slow_mutation_modes<W: SimWorkload>(label: &str, workload: &W) {
    for (mode, faults) in [
        ("slow-only", FaultConfig::slow_mutations()),
        ("combined", FaultConfig::combined(64)),
    ] {
        for seed in [17, 29] {
            let fault_tape = fault_tape(seed);
            let first = record_faults_with_tape(seed, workload, faults, fault_tape.clone());
            let second = record_faults_with_tape(seed, workload, faults, fault_tape);
            assert_no_divergence(&format!("{label}: {mode}, seed {seed}"), &first, &second);
        }
    }
}
