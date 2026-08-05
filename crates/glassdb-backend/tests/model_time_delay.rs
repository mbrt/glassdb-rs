#![cfg(not(sim))]

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::middleware::{DelayBackend, Latency, gcs_delays};
use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
use glassdb_concurr::rt;

#[tokio::test(start_paused = true)]
async fn delay_backend_uses_process_model_time() {
    rt::set_model_time_speedup(5.0).unwrap();
    let mut options = gcs_delays();
    options.obj_read = Latency::new(50, 0);
    let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();

    let wall_start = tokio::time::Instant::now();
    let model_start = rt::Instant::now();
    assert!(matches!(
        backend.read("missing").await,
        Err(BackendError::NotFound)
    ));

    assert!(wall_start.elapsed().abs_diff(Duration::from_millis(10)) <= Duration::from_nanos(1));
    assert!(model_start.elapsed().abs_diff(Duration::from_millis(50)) <= Duration::from_nanos(5));
}
