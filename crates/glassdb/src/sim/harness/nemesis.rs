//! Failure-injection execution for the deterministic simulation harness.
//!
//! The harness chooses modes, streams, seeds, and spawn order. This module owns
//! the resulting faulty transports and executes crash, outage, join, and heal
//! actions. Client cancellation hands control to `ClientRunner`, which owns the
//! corresponding restart lifecycle.

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_backend::middleware::{FaultBackend, FaultOptions};
use glassdb_concurr::{Tape, rt};
use tokio_util::sync::CancellationToken;

/// Owns the fault-injecting transports and ordered client backend views.
pub(super) struct FaultTransports {
    injected: Vec<Arc<FaultBackend>>,
    client_backends: Vec<Arc<dyn Backend>>,
}

impl FaultTransports {
    /// Gives each client a direct view of a faultless backbone.
    pub(super) fn faultless(backbone: &Arc<dyn Backend>, clients: usize) -> Self {
        Self {
            injected: Vec::new(),
            client_backends: (0..clients).map(|_| backbone.clone()).collect(),
        }
    }

    /// Builds active fault transports from harness-selected tape/seed pairs.
    pub(super) fn faulting(
        backbone: &Arc<dyn Backend>,
        intensity: u8,
        schedules: Vec<(Vec<u8>, u64)>,
    ) -> Self {
        let options = FaultOptions::from_intensity(intensity);
        let mut injected = Vec::with_capacity(schedules.len());
        let mut client_backends = Vec::with_capacity(schedules.len());
        for (tape, seed) in schedules {
            let transport = FaultBackend::with_tape(backbone.clone(), tape, seed, options);
            transport.set_active(true);
            injected.push(transport.clone());
            client_backends.push(transport as Arc<dyn Backend>);
        }
        Self {
            injected,
            client_backends,
        }
    }

    /// Transfers client views while retaining injectors for outage and healing.
    pub(super) fn take_client_backends(&mut self) -> Vec<Arc<dyn Backend>> {
        std::mem::take(&mut self.client_backends)
    }

    /// Disables every injector before final verification.
    pub(super) fn final_heal(&self) {
        for transport in &self.injected {
            transport.set_active(false);
        }
    }
}

/// Owns crash and outage task results for one harness run.
pub(super) struct NemesisRunner {
    crash: Option<rt::JoinHandle<()>>,
    outage: Option<rt::JoinHandle<()>>,
}

impl NemesisRunner {
    /// Creates a runner with no selected nemeses.
    pub(super) fn new() -> Self {
        Self {
            crash: None,
            outage: None,
        }
    }

    /// Starts deterministic client-crash injection.
    pub(super) fn spawn_crash(&mut self, signals: &[CancellationToken], intensity: u8, tape: Tape) {
        debug_assert!(self.crash.is_none());
        self.crash = Some(rt::spawn(crash_nemesis(signals.to_vec(), intensity, tape)));
    }

    /// Starts deterministic sustained-outage injection.
    pub(super) fn spawn_outage(&mut self, transports: &FaultTransports, intensity: u8, tape: Tape) {
        debug_assert!(self.outage.is_none());
        self.outage = Some(rt::spawn(outage_nemesis(
            transports.injected.clone(),
            intensity,
            tape,
        )));
    }

    /// Joins selected nemeses in crash-then-outage order and propagates panics.
    pub(super) async fn join(self) {
        if let Some(handle) = self.crash {
            handle.await.expect("crash nemesis task failed");
        }
        if let Some(handle) = self.outage {
            handle.await.expect("outage nemesis task failed");
        }
    }
}

/// Cancels selected clients at deterministic virtual times. Their task owner
/// performs the uncancellable restart without replaying an in-doubt operation.
async fn crash_nemesis(signals: Vec<CancellationToken>, intensity: u8, mut tape: Tape) {
    let crashes = (intensity as usize % 3).min(signals.len());
    for _ in 0..crashes {
        let gap = tape.below(40) + 1;
        rt::sleep(Duration::from_millis(gap)).await;
        let client = tape.below(signals.len() as u64) as usize;
        signals[client].cancel();
    }
}

/// Takes selected client transports down for sustained windows and heals them.
async fn outage_nemesis(transports: Vec<Arc<FaultBackend>>, intensity: u8, mut tape: Tape) {
    if transports.is_empty() {
        return;
    }
    for _ in 0..outage_count(intensity) {
        let gap = tape.below(30) + 1;
        rt::sleep(Duration::from_millis(gap)).await;
        let client = tape.below(transports.len() as u64) as usize;
        transports[client].down();
        // The span keeps retries failing long enough to reach lease recovery.
        let span = tape.below(80) + 20;
        rt::sleep(Duration::from_millis(span)).await;
        transports[client].heal();
    }
}

fn outage_count(intensity: u8) -> usize {
    match intensity {
        0..=47 => 0,
        48..=127 => 1,
        _ => 2,
    }
}
