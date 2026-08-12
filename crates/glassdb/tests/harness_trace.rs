//! Golden migration guards for deterministic harness scheduling traces.

#![cfg(all(sim, feature = "sim"))]

use glassdb::sim::{
    ApiWorkload, HarnessTrace, HarnessTraceEvent, HistoryWorkload, RmwWorkload, TraceClientPhase,
    TraceClientRun, TraceEntropyDraw, TraceEntropySource, TraceNemesisAction, TraceOperationPhase,
    TraceRunPhase, TraceSpawnRole, TraceVerificationPhase, trace_input,
};
use sha2::{Digest, Sha256};

type TraceFn = fn(&[u8]) -> HarnessTrace;

struct TapeBaseline {
    name: &'static str,
    input: &'static [u8],
    trace: TraceFn,
    sha256: &'static str,
    expectation: Expectation,
}

#[derive(Clone, Copy)]
enum Expectation {
    Normal,
    FaultRecovery,
}

fn rmw_trace(input: &[u8]) -> HarnessTrace {
    trace_input::<RmwWorkload>(input)
}

fn history_trace(input: &[u8]) -> HarnessTrace {
    trace_input::<HistoryWorkload>(input)
}

fn api_trace(input: &[u8]) -> HarnessTrace {
    trace_input::<ApiWorkload>(input)
}

// Copied from the named committed corpus entries so corpus minimization cannot
// silently replace a migration guard. Each basename is the SHA-1 of its bytes.
// Source: fuzz/corpus/concurrent_tx/5854e97b045d69a9995d95cea2295580a2a3dd20
const RMW_NORMAL: &[u8] = &[
    0, 255, 1, 255, 0, 255, 0, 255, 0, 255, 1, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 255, 0,
];

// Source: fuzz/corpus/history/3288167f0c39670d2799cf119bb80493e6aa860b
const HISTORY_FAULT_RECOVERY: &[u8] = &[
    115, 34, 206, 95, 63, 145, 236, 255, 112, 143, 239, 101, 6, 83, 13, 11, 255, 255, 255, 10, 255,
    255, 255, 255, 251, 1, 80, 63, 13, 52, 114, 241, 99, 49, 101, 100, 244, 101, 100, 244,
];

// Source: fuzz/corpus/api_correctness/c2cf339430f079f0ddd1dca379e626f897fec21f
const API_NORMAL: &[u8] = &[
    97, 112, 105, 122, 122, 122, 122, 122, 45, 99, 111, 121, 97, 112, 105, 45, 99, 111, 114, 114,
    101, 99, 116, 110, 101, 51, 64, 115, 115, 114, 101, 99, 116, 109, 112, 116, 121,
];
const TAPE_BASELINES: &[TapeBaseline] = &[
    TapeBaseline {
        name: "rmw normal",
        input: RMW_NORMAL,
        trace: rmw_trace,
        sha256: "bfeb54d5206ef83932657f667a94470a91662315d3d4d3161c2f731920a34b09",
        expectation: Expectation::Normal,
    },
    TapeBaseline {
        name: "history fault recovery",
        input: HISTORY_FAULT_RECOVERY,
        trace: history_trace,
        sha256: "fae678c79134297f70f40a8f4daf9bffacb044ff1d549cc131643d4c6321011a",
        expectation: Expectation::FaultRecovery,
    },
    TapeBaseline {
        name: "API normal",
        input: API_NORMAL,
        trace: api_trace,
        sha256: "680ae361aeddab1d3c334c55f11b04969237092f114c8d5a88302a15eef27b54",
        expectation: Expectation::Normal,
    },
];

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

struct RunSegment<'a> {
    cached: bool,
    events: &'a [HarnessTraceEvent],
}

fn run_segments<'a>(name: &str, trace: &'a HarnessTrace) -> Vec<RunSegment<'a>> {
    let events = trace.events();
    let mut open = None;
    let mut segments = Vec::with_capacity(2);
    for (index, event) in events.iter().enumerate() {
        match event {
            HarnessTraceEvent::Run {
                cached,
                phase: TraceRunPhase::Started,
            } => {
                assert!(
                    open.replace((*cached, index + 1)).is_none(),
                    "{name}: nested run start"
                );
            }
            HarnessTraceEvent::Run {
                cached,
                phase: TraceRunPhase::Finished,
            } => {
                let (started_cached, start) = open
                    .take()
                    .unwrap_or_else(|| panic!("{name}: run finish without start"));
                assert_eq!(
                    *cached, started_cached,
                    "{name}: run changed cache mode before finishing"
                );
                segments.push(RunSegment {
                    cached: *cached,
                    events: &events[start..index],
                });
            }
            _ => {}
        }
    }
    assert!(open.is_none(), "{name}: trace ended with an unfinished run");
    assert_eq!(
        segments
            .iter()
            .map(|segment| segment.cached)
            .collect::<Vec<_>>(),
        vec![false, true],
        "{name}: expected complete cache-free then cached runs"
    );
    segments
}

fn assert_run_boundaries(name: &str, trace: &HarnessTrace) {
    let events = trace.events();
    assert!(
        matches!(
            events.first(),
            Some(HarnessTraceEvent::Run {
                cached: false,
                phase: TraceRunPhase::Started,
            })
        ),
        "{name}: trace must start with the cache-free run"
    );
    assert!(
        matches!(
            events.last(),
            Some(HarnessTraceEvent::Run {
                cached: true,
                phase: TraceRunPhase::Finished,
            })
        ),
        "{name}: trace must finish with the cached run"
    );

    for segment in run_segments(name, trace) {
        let body = segment.events;
        let verification: Vec<_> = body
            .iter()
            .filter_map(|event| match event {
                HarnessTraceEvent::Verification { phase } => Some(*phase),
                _ => None,
            })
            .collect();
        assert_eq!(
            verification,
            vec![
                TraceVerificationPhase::Started,
                TraceVerificationPhase::Finished,
            ],
            "{name}: final verification boundaries changed"
        );
        assert!(
            matches!(
                body.last(),
                Some(HarnessTraceEvent::Verification {
                    phase: TraceVerificationPhase::Finished,
                })
            ),
            "{name}: run finished before final verification"
        );
    }
}

fn assert_normal(name: &str, trace: &HarnessTrace) {
    let events = trace.events();
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessTraceEvent::Operation {
                phase: TraceOperationPhase::Succeeded,
                ..
            }
        )),
        "{name}: normal fixture performs no successful operation"
    );
    assert!(
        events.iter().all(|event| !matches!(
            event,
            HarnessTraceEvent::Operation {
                phase: TraceOperationPhase::AdmissibleError,
                ..
            } | HarnessTraceEvent::Nemesis { .. }
        )),
        "{name}: normal fixture unexpectedly exercises a fault"
    );
    for role in [TraceSpawnRole::CrashNemesis, TraceSpawnRole::OutageNemesis] {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessTraceEvent::SpawnDecision {
                    role: actual,
                    spawned: false,
                } if *actual == role
            )),
            "{name}: normal fixture did not record the disabled nemesis"
        );
    }
}

fn assert_fault_recovery(name: &str, trace: &HarnessTrace) {
    let events = trace.events();
    for role in [TraceSpawnRole::CrashNemesis, TraceSpawnRole::OutageNemesis] {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessTraceEvent::SpawnDecision {
                    role: actual,
                    spawned: true,
                } if *actual == role
            )),
            "{name}: fault fixture did not spawn {role:?}"
        );
    }
    assert!(
        events.iter().any(|event| matches!(
            event,
            HarnessTraceEvent::Operation {
                phase: TraceOperationPhase::AdmissibleError,
                ..
            }
        )),
        "{name}: fault fixture produced no admissible client error"
    );
    for action in [
        TraceNemesisAction::Crash,
        TraceNemesisAction::Down,
        TraceNemesisAction::Heal,
        TraceNemesisAction::FinalHeal,
    ] {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessTraceEvent::Nemesis { action: actual, .. } if *actual == action
            )),
            "{name}: fault fixture did not record {action:?}"
        );
    }

    let segments = run_segments(name, trace);
    let healed = segments.iter().any(|segment| {
        let events = segment.events;
        events.iter().enumerate().any(|(down_index, event)| {
            let HarnessTraceEvent::Nemesis {
                action: TraceNemesisAction::Down,
                target,
                ..
            } = event
            else {
                return false;
            };
            events[down_index + 1..].iter().any(|later| {
                matches!(
                    later,
                    HarnessTraceEvent::Nemesis {
                        action: TraceNemesisAction::Heal,
                        target: later_target,
                        ..
                    } if later_target == target
                )
            })
        })
    });
    assert!(
        healed,
        "{name}: no outage target was healed after going down"
    );
    assert!(
        segments.iter().all(|segment| {
            let events = segment.events;
            let final_heal = events.iter().position(|event| {
                matches!(
                    event,
                    HarnessTraceEvent::Nemesis {
                        action: TraceNemesisAction::FinalHeal,
                        ..
                    }
                )
            });
            let verification = events.iter().position(|event| {
                matches!(
                    event,
                    HarnessTraceEvent::Verification {
                        phase: TraceVerificationPhase::Started,
                    }
                )
            });
            matches!((final_heal, verification), (Some(heal), Some(verify)) if heal < verify)
        }),
        "{name}: a run verified before its final healing pass"
    );

    let restarted = segments.iter().any(|segment| {
        let events = segment.events;
        events.iter().enumerate().any(|(crash_index, event)| {
            let HarnessTraceEvent::Client {
                client,
                phase: TraceClientPhase::Crashed,
                ..
            } = event
            else {
                return false;
            };
            let Some(restart_offset) = events[crash_index + 1..].iter().position(|later| {
                matches!(
                    later,
                    HarnessTraceEvent::Client {
                        client: later_client,
                        phase: TraceClientPhase::Restarted,
                        ..
                    } if later_client == client
                )
            }) else {
                return false;
            };
            events[crash_index + restart_offset + 2..]
                .iter()
                .any(|later| {
                    matches!(
                        later,
                        HarnessTraceEvent::Operation {
                            client: later_client,
                            run: TraceClientRun::Restart,
                            ..
                        } if later_client == client
                    )
                })
        })
    });
    assert!(
        restarted,
        "{name}: no crashed client resumed its operation stream"
    );
}

#[test]
fn tape_traces_match_reviewed_baselines() {
    for baseline in TAPE_BASELINES {
        let first = (baseline.trace)(baseline.input);
        let first_bytes = first.canonical_bytes();
        let second_bytes = (baseline.trace)(baseline.input).canonical_bytes();
        assert!(
            first_bytes == second_bytes,
            "{}: repeated runs produced different canonical traces",
            baseline.name
        );

        assert_run_boundaries(baseline.name, &first);
        for segment in run_segments(baseline.name, &first) {
            assert!(
                segment.events.iter().any(|event| matches!(
                    event,
                    HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
                        source: TraceEntropySource::SchedulerInput,
                        ..
                    })
                )),
                "{}: one cache mode did not exercise the tape scheduler",
                baseline.name
            );
        }
        match baseline.expectation {
            Expectation::Normal => assert_normal(baseline.name, &first),
            Expectation::FaultRecovery => assert_fault_recovery(baseline.name, &first),
        }

        let actual_digest = digest(&first_bytes);
        assert_eq!(
            actual_digest,
            baseline.sha256,
            "{}: reviewed trace changed ({} events, {} canonical bytes)",
            baseline.name,
            first.events().len(),
            first_bytes.len()
        );
    }
}
