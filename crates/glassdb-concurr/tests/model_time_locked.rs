#![cfg(not(sim))]

use glassdb_concurr::rt::{self, ModelTimeError};

#[test]
fn observing_time_locks_the_default_configuration() {
    let _ = rt::Instant::now();
    assert_eq!(
        rt::set_model_time_speedup(2.0),
        Err(ModelTimeError::AlreadyInitialized)
    );
}
