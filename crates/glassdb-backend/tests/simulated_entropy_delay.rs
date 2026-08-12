#![cfg(sim)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_backend::middleware::{DelayBackend, Latency, gcs_delays};
use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
use glassdb_concurr::rt::{
    self, RuntimeEntropySource, RuntimeTraceEvent, TapeScheduler, block_on_with_trace,
};

fn replay(seed: u64) -> (Duration, Vec<RuntimeTraceEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let observer = {
        let events = events.clone();
        Arc::new(move |event| events.lock().unwrap().push(event))
    };
    let elapsed = block_on_with_trace(TapeScheduler::new(Vec::new()), seed, observer, async {
        let mut options = gcs_delays();
        options.latency.obj_read = Latency::new(57, 7);
        let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();
        let start = rt::Instant::now();
        assert!(matches!(
            backend.read("missing").await,
            Err(BackendError::NotFound)
        ));
        start.elapsed()
    });
    let trace = events.lock().unwrap().clone();
    (elapsed, trace)
}

#[test]
fn delay_sampling_replays_from_the_executor_entropy_stream() {
    let first = replay(0xF2_5C);
    let repeated = replay(0xF2_5C);
    assert_eq!(first, repeated);

    let first_draw = first
        .1
        .iter()
        .position(|event| {
            matches!(
                event,
                RuntimeTraceEvent::EntropyDraw {
                    source: RuntimeEntropySource::FillRandom,
                    bytes,
                } if bytes.len() == 8
            )
        })
        .expect("delay sampling did not consume executor entropy");
    assert_eq!(
        first_draw, 2,
        "latency entropy moved past its call boundary"
    );

    let different_seed = replay(0xF2_5D);
    assert_ne!(first.1, different_seed.1);
    assert_ne!(first.0, different_seed.0);
}
