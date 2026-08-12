//! Versioned semantic trace for deterministic harness migration guards.
//!
//! The schema is deliberately test-only and contains no frozen corpus values.
//! F29-B/F29-C can add reviewed encodings and digests without changing the
//! harness observation points introduced here.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};

#[cfg(sim)]
use glassdb_concurr::rt;
use serde::Serialize;

/// Current structured harness-trace schema.
pub const HARNESS_TRACE_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HarnessTrace {
    pub(super) events: Vec<HarnessTraceEvent>,
}

impl HarnessTrace {
    pub fn schema_version(&self) -> u8 {
        HARNESS_TRACE_SCHEMA_VERSION
    }

    pub fn events(&self) -> &[HarnessTraceEvent] {
        &self.events
    }

    /// Returns a canonical JSON encoding prefixed by the numeric schema
    /// version. The schema contains only ordered sequences and integer data.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&(HARNESS_TRACE_SCHEMA_VERSION, &self.events)).unwrap()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HarnessTraceEvent {
    Run {
        cached: bool,
        phase: TraceRunPhase,
    },
    SpawnDecision {
        role: TraceSpawnRole,
        spawned: bool,
    },
    TaskSpawned {
        task_id: u64,
    },
    TaskSelected {
        task_id: u64,
    },
    EntropyDraw(TraceEntropyDraw),
    Client {
        client: u32,
        phase: TraceClientPhase,
        consumed: u32,
    },
    Operation {
        client: u32,
        operation: u32,
        run: TraceClientRun,
        phase: TraceOperationPhase,
    },
    Nemesis {
        nemesis: TraceNemesis,
        action: TraceNemesisAction,
        target: Option<u32>,
        delay_ms: Option<u64>,
    },
    Verification {
        phase: TraceVerificationPhase,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceRunPhase {
    Started,
    Finished,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TraceSpawnRole {
    Client(u32),
    Observer,
    CrashNemesis,
    OutageNemesis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum TraceEntropyDraw {
    Bytes {
        source: TraceEntropySource,
        bytes: Vec<u8>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceEntropySource {
    Runtime,
    TapeInput,
    TapeFallbackRng,
    SchedulerInput,
    SchedulerRng,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceClientPhase {
    Started,
    OpenFailed,
    Crashed,
    Restarted,
    RestartOpenFailed,
    Finished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceClientRun {
    Initial,
    Restart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceOperationPhase {
    Started,
    Succeeded,
    AdmissibleError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceNemesis {
    Crash,
    Outage,
    Harness,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceNemesisAction {
    Crash,
    Down,
    Heal,
    FinalHeal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TraceVerificationPhase {
    Started,
    Finished,
}

#[derive(Clone, Default)]
pub(super) struct TraceSink(Option<Arc<Mutex<Vec<HarnessTraceEvent>>>>);

impl TraceSink {
    #[cfg(sim)]
    pub(super) fn enabled() -> Self {
        Self(Some(Arc::new(Mutex::new(Vec::new()))))
    }

    pub(super) fn record(&self, event: HarnessTraceEvent) {
        if let Some(events) = &self.0 {
            events.lock().unwrap().push(event);
        }
    }

    pub(super) fn spawn(&self, role: TraceSpawnRole, spawned: bool) {
        self.record(HarnessTraceEvent::SpawnDecision { role, spawned });
    }

    pub(super) fn client(&self, client: usize, phase: TraceClientPhase, consumed: usize) {
        self.record(HarnessTraceEvent::Client {
            client: client as u32,
            phase,
            consumed: consumed as u32,
        });
    }

    pub(super) fn operation(
        &self,
        client: u32,
        operation: u32,
        run: TraceClientRun,
        phase: TraceOperationPhase,
    ) {
        self.record(HarnessTraceEvent::Operation {
            client,
            operation,
            run,
            phase,
        });
    }

    pub(super) fn nemesis(
        &self,
        nemesis: TraceNemesis,
        action: TraceNemesisAction,
        target: Option<usize>,
        delay_ms: Option<u64>,
    ) {
        self.record(HarnessTraceEvent::Nemesis {
            nemesis,
            action,
            target: target.map(|target| target as u32),
            delay_ms,
        });
    }

    pub(super) fn verification(&self, phase: TraceVerificationPhase) {
        self.record(HarnessTraceEvent::Verification { phase });
    }

    #[cfg(sim)]
    pub(super) fn finish(&self) -> HarnessTrace {
        HarnessTrace {
            events: self.0.as_ref().unwrap().lock().unwrap().clone(),
        }
    }

    #[cfg(sim)]
    pub(super) fn runtime_observer(&self) -> rt::RuntimeTraceObserver {
        let trace = self.clone();
        Arc::new(move |event| {
            trace.record(match event {
                rt::RuntimeTraceEvent::TaskSpawned { task_id } => {
                    HarnessTraceEvent::TaskSpawned { task_id }
                }
                rt::RuntimeTraceEvent::TaskSelected { task_id } => {
                    HarnessTraceEvent::TaskSelected { task_id }
                }
                rt::RuntimeTraceEvent::EntropyDraw { source, bytes } => {
                    HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
                        source: match source {
                            rt::RuntimeEntropySource::FillRandom => TraceEntropySource::Runtime,
                            rt::RuntimeEntropySource::TapeInput => TraceEntropySource::TapeInput,
                            rt::RuntimeEntropySource::TapeFallbackRng => {
                                TraceEntropySource::TapeFallbackRng
                            }
                            rt::RuntimeEntropySource::SchedulerInput => {
                                TraceEntropySource::SchedulerInput
                            }
                            rt::RuntimeEntropySource::SchedulerRng => {
                                TraceEntropySource::SchedulerRng
                            }
                        },
                        bytes,
                    })
                }
            });
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct ClientOperationTrace<'a> {
    sink: &'a TraceSink,
    client: u32,
    base: usize,
    run: TraceClientRun,
}

impl<'a> ClientOperationTrace<'a> {
    pub(super) fn new(
        sink: &'a TraceSink,
        client: usize,
        base: usize,
        run: TraceClientRun,
    ) -> Self {
        Self {
            sink,
            client: client as u32,
            base,
            run,
        }
    }

    pub(super) fn record(self, offset: usize, phase: TraceOperationPhase) {
        self.sink
            .operation(self.client, (self.base + offset) as u32, self.run, phase);
    }
}
