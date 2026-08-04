#![cfg(not(sim))]

use std::time::Duration;

use glassdb_concurr::rt::{self, ModelTimeError};

#[tokio::test(start_paused = true)]
async fn accelerated_time_is_coherent_and_immutable() {
    for speedup in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::MAX] {
        assert!(matches!(
            rt::set_model_time_speedup(speedup),
            Err(ModelTimeError::InvalidSpeedup(value)) if value.to_bits() == speedup.to_bits()
        ));
    }
    rt::set_model_time_speedup(5.0).unwrap();

    let runtime_start = tokio::time::Instant::now();
    let model_start = rt::Instant::now();
    let system_start = rt::system_now();
    rt::sleep(Duration::from_secs(5)).await;

    assert_eq!(runtime_start.elapsed(), Duration::from_secs(1));
    assert_eq!(model_start.elapsed(), Duration::from_secs(5));
    assert_eq!(
        rt::system_now().duration_since(system_start).unwrap(),
        Duration::from_secs(5)
    );

    let runtime_start = tokio::time::Instant::now();
    assert_eq!(
        rt::timeout(Duration::from_secs(10), std::future::pending::<()>()).await,
        Err(rt::TimedOut)
    );
    assert_eq!(runtime_start.elapsed(), Duration::from_secs(2));
    assert_eq!(
        rt::set_model_time_speedup(5.0),
        Err(ModelTimeError::AlreadyInitialized)
    );
}
