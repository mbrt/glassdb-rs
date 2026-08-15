#![cfg(sim)]

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::middleware::{DelayBackend, Latency, gcs_delays};
use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
use glassdb_concurr::exec::{TapeScheduler, block_on_with};
use glassdb_concurr::{entropy, rt};

fn replay(seed: u64) -> (Duration, [u8; 8]) {
    block_on_with(TapeScheduler::new(Vec::new()), seed, async {
        let mut options = gcs_delays();
        options.latency.obj_read = Latency::new(57, 7);
        let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();
        let start = rt::Instant::now();
        assert!(matches!(
            backend.read("missing").await,
            Err(BackendError::NotFound)
        ));
        let elapsed = start.elapsed();
        let mut entropy_sentinel = [0; 8];
        entropy::fill_bytes(&mut entropy_sentinel);
        (elapsed, entropy_sentinel)
    })
}

#[test]
fn delay_sampling_replays_from_the_executor_entropy_stream() {
    let first = replay(0xF2_5C);
    let repeated = replay(0xF2_5C);
    assert_eq!(first, repeated);
    assert_eq!(first.1, [213, 85, 126, 142, 115, 101, 39, 176]);

    let different_seed = replay(0xF2_5D);
    assert_ne!(first.0, different_seed.0);
    assert_ne!(first.1, different_seed.1);
}
