//! Semantic regression coverage for deterministic harness scheduling traces.

#![cfg(all(sim, feature = "sim"))]

use glassdb::sim::{
    FaultConfig, HarnessTrace, HarnessTraceEvent, HistoryWorkload, PCT_DEFAULT_DEPTH,
    PCT_DEFAULT_STEPS, RmwOp, RmwWorkload, TraceClientPhase, TraceClientRun, TraceEntropyDraw,
    TraceEntropySource, TraceNemesisAction, TraceOperationPhase, TraceRunPhase, TraceSpawnRole,
    TraceVerificationPhase, pct_trace, trace_input,
};
type TraceFn = fn(&[u8]) -> HarnessTrace;

struct TapeFixture {
    name: &'static str,
    input: &'static [u8],
    trace: TraceFn,
    expectation: Expectation,
}

struct PctFixture {
    name: &'static str,
    seed: u64,
    change_points: [u64; 2],
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

// Copied from named committed corpus entries so minimization cannot silently
// remove the semantic scenarios. Each basename is the SHA-1 of its bytes.
// Source: fuzz/corpus/concurrent_tx/5854e97b045d69a9995d95cea2295580a2a3dd20
const RMW_NORMAL: &[u8] = &[
    0, 255, 1, 255, 0, 255, 0, 255, 0, 255, 1, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 255, 0,
];

// Source: fuzz/corpus/history/3288167f0c39670d2799cf119bb80493e6aa860b
const HISTORY_FAULT_RECOVERY: &[u8] = &[
    115, 34, 206, 95, 63, 145, 236, 255, 112, 143, 239, 101, 6, 83, 13, 11, 255, 255, 255, 10, 255,
    255, 255, 255, 251, 1, 80, 63, 13, 52, 114, 241, 99, 49, 101, 100, 244, 101, 100, 244,
];

const TAPE_FIXTURES: &[TapeFixture] = &[
    TapeFixture {
        name: "rmw normal",
        input: RMW_NORMAL,
        trace: rmw_trace,
        expectation: Expectation::Normal,
    },
    TapeFixture {
        name: "history fault recovery",
        input: HISTORY_FAULT_RECOVERY,
        trace: history_trace,
        expectation: Expectation::FaultRecovery,
    },
];

const PCT_FIXTURES: &[PctFixture] = &[
    PctFixture {
        name: "early PCT boundary",
        seed: 12_780,
        change_points: [1, 15],
    },
    PctFixture {
        name: "later PCT boundaries",
        seed: 12_980,
        change_points: [9, 29],
    },
];

const TAPE_RUN_MODES: &[bool] = &[false, true];
const PCT_RUN_MODES: &[bool] = &[false];

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
fn tape_fixtures_replay_with_semantic_boundaries() {
    for fixture in TAPE_FIXTURES {
        let first = (fixture.trace)(fixture.input);
        let first_bytes = first.canonical_bytes();
        let second_bytes = (fixture.trace)(fixture.input).canonical_bytes();
        assert!(
            first_bytes == second_bytes,
            "{}: repeated runs produced different canonical traces",
            fixture.name
        );

        assert_run_boundaries(fixture.name, &first, TAPE_RUN_MODES);
        for segment in run_segments(fixture.name, &first, TAPE_RUN_MODES) {
            assert!(
                segment.events.iter().any(|event| matches!(
                    event,
                    HarnessTraceEvent::EntropyDraw(TraceEntropyDraw::Bytes {
                        source: TraceEntropySource::SchedulerInput,
                        ..
                    })
                )),
                "{}: one cache mode did not exercise the tape scheduler",
                fixture.name
            );
        }
        match fixture.expectation {
            Expectation::Normal => assert_normal(fixture.name, &first),
            Expectation::FaultRecovery => assert_fault_recovery(fixture.name, &first),
        }
    }
}

#[test]
fn pct_seeded_traces_replay_and_diverge_with_expected_boundaries() {
    let workload = pct_workload();
    let mut canonical = Vec::with_capacity(PCT_FIXTURES.len());
    for fixture in PCT_FIXTURES {
        let first = pct_trace(&workload, FaultConfig::failures(7), fixture.seed);
        let first_bytes = first.canonical_bytes();
        let second_bytes =
            pct_trace(&workload, FaultConfig::failures(7), fixture.seed).canonical_bytes();
        assert!(
            first_bytes == second_bytes,
            "{}: repeated seed {} runs produced different canonical traces",
            fixture.name,
            fixture.seed
        );

        assert_pct_semantics(fixture.name, &first, fixture.change_points);
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
