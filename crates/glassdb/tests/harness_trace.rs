//! Golden migration guards for deterministic harness scheduling traces.

#![cfg(all(sim, feature = "sim"))]

use glassdb::sim::{
    ApiWorkload, FaultConfig, HarnessTrace, HarnessTraceEvent, HistoryWorkload, PCT_DEFAULT_DEPTH,
    PCT_DEFAULT_STEPS, RmwOp, RmwWorkload, TraceClientPhase, TraceClientRun, TraceEntropyDraw,
    TraceEntropySource, TraceNemesisAction, TraceOperationPhase, TraceRunPhase, TraceSpawnRole,
    TraceVerificationPhase, pct_trace, trace_input,
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

struct PctBaseline {
    name: &'static str,
    seed: u64,
    change_points: [u64; 2],
    sha256: &'static str,
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

const PCT_BASELINES: &[PctBaseline] = &[
    PctBaseline {
        name: "early PCT boundary",
        seed: 12_780,
        change_points: [1, 15],
        sha256: "fc8e2558ae3e5db6ff95905e9925197887232386730961ccf9124102d4a412fb",
    },
    PctBaseline {
        name: "later PCT boundaries",
        seed: 12_980,
        change_points: [9, 29],
        sha256: "7a4a21cc10d02d0d4af476056c7f9f10622469ff3a21a5ec3a5741184bdfd7ea",
    },
];

const TAPE_RUN_MODES: &[bool] = &[false, true];
const PCT_RUN_MODES: &[bool] = &[false];

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

struct RunSegment<'a> {
    cached: bool,
    events: &'a [HarnessTraceEvent],
}

fn run_segments<'a>(
    name: &str,
    trace: &'a HarnessTrace,
    expected_modes: &[bool],
) -> Vec<RunSegment<'a>> {
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
        expected_modes,
        "{name}: run modes changed"
    );
    segments
}

fn assert_run_boundaries(name: &str, trace: &HarnessTrace, expected_modes: &[bool]) {
    let events = trace.events();
    let first_mode = *expected_modes.first().expect("expected at least one run");
    let last_mode = *expected_modes.last().unwrap();
    assert!(
        matches!(
            events.first(),
            Some(HarnessTraceEvent::Run {
                cached,
                phase: TraceRunPhase::Started,
            }) if *cached == first_mode
        ),
        "{name}: trace did not start with its expected run mode"
    );
    assert!(
        matches!(
            events.last(),
            Some(HarnessTraceEvent::Run {
                cached,
                phase: TraceRunPhase::Finished,
            }) if *cached == last_mode
        ),
        "{name}: trace did not finish with its expected run mode"
    );

    for segment in run_segments(name, trace, expected_modes) {
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

    let segments = run_segments(name, trace, TAPE_RUN_MODES);
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

fn pct_workload() -> RmwWorkload {
    RmwWorkload {
        clients: vec![
            vec![RmwOp::Rmw(0), RmwOp::ReadOnly(vec![0])],
            vec![RmwOp::Rmw(1)],
        ],
    }
}

fn pct_scheduler_draw(name: &str, event: &HarnessTraceEvent) -> Option<u64> {
    let HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
        source: TraceEntropySource::SchedulerRng,
        bytes,
    }) = event
    else {
        return None;
    };
    let bytes: [u8; 8] = bytes
        .as_slice()
        .try_into()
        .unwrap_or_else(|_| panic!("{name}: PCT scheduler draw was not eight bytes"));
    Some(u64::from_le_bytes(bytes))
}

fn assert_pct_semantics(name: &str, trace: &HarnessTrace, expected_change_points: [u64; 2]) {
    assert_run_boundaries(name, trace, PCT_RUN_MODES);
    let segments = run_segments(name, trace, PCT_RUN_MODES);
    let events = segments[0].events;

    let first_spawn = events
        .iter()
        .position(|event| matches!(event, HarnessTraceEvent::TaskSpawned { .. }))
        .unwrap_or_else(|| panic!("{name}: PCT trace spawned no task"));
    let priority_draw = first_spawn
        .checked_sub(1)
        .unwrap_or_else(|| panic!("{name}: first task had no priority draw"));
    assert!(
        pct_scheduler_draw(name, &events[priority_draw]).is_some(),
        "{name}: first task was not preceded by its priority draw"
    );
    let change_draws: Vec<_> = events[..priority_draw]
        .iter()
        .map(|event| {
            pct_scheduler_draw(name, event)
                .unwrap_or_else(|| panic!("{name}: event preceded the PCT change-point draws"))
        })
        .collect();
    assert_eq!(
        change_draws.len(),
        PCT_DEFAULT_DEPTH.saturating_sub(1),
        "{name}: PCT consumed the wrong number of change-point draws"
    );
    let change_points: Vec<_> = change_draws
        .iter()
        .map(|draw| 1 + draw % PCT_DEFAULT_STEPS.max(1))
        .collect();
    assert_eq!(
        change_points, expected_change_points,
        "{name}: seeded PCT change points moved"
    );

    let mut next_task = 0;
    let mut selections = 0;
    for (index, event) in events.iter().enumerate() {
        match event {
            HarnessTraceEvent::TaskSpawned { task_id } => {
                assert_eq!(
                    *task_id, next_task,
                    "{name}: executor task spawn order changed"
                );
                assert!(
                    index > 0 && pct_scheduler_draw(name, &events[index - 1]).is_some(),
                    "{name}: task {task_id} did not consume one priority draw before spawning"
                );
                next_task += 1;
            }
            HarnessTraceEvent::TaskSelected { task_id } => {
                assert!(
                    *task_id < next_task,
                    "{name}: scheduler selected task {task_id} before it spawned"
                );
                selections += 1;
            }
            _ => {}
        }
    }
    let scheduler_draws = events
        .iter()
        .filter(|event| pct_scheduler_draw(name, event).is_some())
        .count();
    assert_eq!(
        scheduler_draws,
        next_task as usize + PCT_DEFAULT_DEPTH.saturating_sub(1),
        "{name}: PCT priority entropy no longer has one draw per spawned task"
    );
    assert!(
        selections > *expected_change_points.iter().max().unwrap(),
        "{name}: {selections} selections ended before both change-point boundaries"
    );

    let roles: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            HarnessTraceEvent::SpawnDecision { role, spawned } => Some((role.clone(), *spawned)),
            _ => None,
        })
        .collect();
    assert_eq!(
        roles,
        vec![
            (TraceSpawnRole::Client(0), true),
            (TraceSpawnRole::Client(1), true),
            (TraceSpawnRole::Observer, false),
            (TraceSpawnRole::CrashNemesis, true),
            (TraceSpawnRole::OutageNemesis, true),
        ],
        "{name}: harness role spawn order changed"
    );
    for source in [
        TraceEntropySource::Runtime,
        TraceEntropySource::TapeFallbackRng,
        TraceEntropySource::SchedulerRng,
    ] {
        assert!(
            events.iter().any(|event| matches!(
                event,
                HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
                    source: actual,
                    ..
                }) if *actual == source
            )),
            "{name}: PCT trace did not consume {source:?} entropy"
        );
    }
    assert!(
        events.iter().all(|event| !matches!(
            event,
            HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
                source: TraceEntropySource::SchedulerInput | TraceEntropySource::TapeInput,
                ..
            })
        )),
        "{name}: PCT trace unexpectedly consumed a supplied tape"
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

        assert_run_boundaries(baseline.name, &first, TAPE_RUN_MODES);
        for segment in run_segments(baseline.name, &first, TAPE_RUN_MODES) {
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

#[test]
fn pct_traces_match_reviewed_baselines() {
    let workload = pct_workload();
    let mut canonical = Vec::with_capacity(PCT_BASELINES.len());
    for baseline in PCT_BASELINES {
        let first = pct_trace(&workload, FaultConfig::failures(7), baseline.seed);
        let first_bytes = first.canonical_bytes();
        let second_bytes =
            pct_trace(&workload, FaultConfig::failures(7), baseline.seed).canonical_bytes();
        assert!(
            first_bytes == second_bytes,
            "{}: repeated seed {} runs produced different canonical traces",
            baseline.name,
            baseline.seed
        );

        assert_pct_semantics(baseline.name, &first, baseline.change_points);
        let actual_digest = digest(&first_bytes);
        assert_eq!(
            actual_digest,
            baseline.sha256,
            "{}: reviewed seed {} trace changed ({} events, {} canonical bytes)",
            baseline.name,
            baseline.seed,
            first.events().len(),
            first_bytes.len()
        );
        canonical.push(first_bytes);
    }

    for left in 0..canonical.len() {
        for right in left + 1..canonical.len() {
            assert_ne!(
                canonical[left], canonical[right],
                "selected distinct PCT seeds produced the same trace"
            );
        }
    }
}
