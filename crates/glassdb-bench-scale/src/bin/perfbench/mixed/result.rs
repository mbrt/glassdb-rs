use std::time::Duration;

use serde::Serialize;

use glassdb::Stats;
use glassdb_bench_scale::bench::Results;

use super::options::CellDimension;

/// One shape's fixed timing observations for a cell report.
pub(super) struct ShapeMeasurement {
    shape: &'static str,
    results: Results,
}

impl ShapeMeasurement {
    /// Associates timing observations with their serialized shape name.
    pub(super) fn new(shape: &'static str, results: Results) -> Self {
        Self { shape, results }
    }
}

/// Non-measured attributes included in a cell report.
pub(super) struct CellMetadata {
    mode: &'static str,
    affinity_pct: u8,
    database_limit: usize,
    databases: usize,
    workers_per_shape: usize,
    setup_splits: u64,
    split_settle_elapsed: Duration,
    policy: String,
    restructure: Restructure,
}

/// Splits and merges of the measurement clients.
#[derive(Debug, Clone, Copy, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Restructure {
    warmup_splits: u64,
    warmup_merges: u64,
    measured_splits: u64,
    measured_merges: u64,
}

impl CellMetadata {
    /// Captures the identity and setup outcome of a benchmark cell.
    pub(super) fn new(
        mode: &'static str,
        affinity_pct: u8,
        database_limit: usize,
        databases: usize,
        workers_per_shape: usize,
        setup_splits: u64,
        split_settle_elapsed: Duration,
    ) -> Self {
        Self {
            mode,
            affinity_pct,
            database_limit,
            databases,
            workers_per_shape,
            setup_splits,
            split_settle_elapsed,
            policy: "engine".into(),
            restructure: Restructure::default(),
        }
    }

    /// Records the topology policy of the measurement clients, and their
    /// changes of the tree before measurement.
    pub(super) fn with_policy(self, policy: String, restructure: Restructure) -> Self {
        Self {
            policy,
            restructure,
            ..self
        }
    }
}

impl Restructure {
    /// Adds the changes of one client during warmup.
    pub(super) fn add_warmup(&mut self, delta: Stats) {
        self.warmup_splits += delta.restructurer.splits;
        self.warmup_merges += delta.restructurer.merges;
    }

    fn add_measured(mut self, delta: Stats) -> Self {
        self.measured_splits += delta.restructurer.splits;
        self.measured_merges += delta.restructurer.merges;
        self
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunResult {
    run: usize,
    cells: Vec<CellResult>,
}

impl RunResult {
    /// Creates the serialized report for one complete dimension sweep.
    pub(super) fn new(run: usize, cells: Vec<CellResult>) -> Self {
        Self { run, cells }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CellResult {
    mode: String,
    affinity_pct: u8,
    database_limit: usize,
    databases: usize,
    workers_per_shape: usize,
    setup_splits: u64,
    split_settle_wall_ms: u64,
    failures: u64,
    policy: String,
    restructure: Restructure,
    shapes: Vec<ShapeResult>,
    aggregate_ops: OpsPerTx,
    aggregate_protocol: ProtocolPerTx,
    avoidable_per_tx: AvoidablePerTx,
}

/// Avoidable time of ADR-074 in milliseconds per transaction, by cause.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct AvoidablePerTx {
    lost_cas_ms: f64,
    queue_wait_ms: f64,
    slow_cas_ms: f64,
    inline_pressure_ms: f64,
    adjacent_miss_ms: f64,
    scan_crossing_ms: f64,
}

impl CellResult {
    /// Summarizes fixed timing samples and database counter deltas for one cell.
    pub(super) fn summarize(
        metadata: CellMetadata,
        measurements: impl IntoIterator<Item = ShapeMeasurement>,
        deltas: &[Stats],
        target: u64,
    ) -> Self {
        let shapes: Vec<_> = measurements
            .into_iter()
            .map(|measurement| ShapeResult::summarize(measurement, target))
            .collect();
        let logical_txn = shapes.iter().map(|shape| shape.committed as u64).sum();
        let raw_ops = deltas
            .iter()
            .copied()
            .fold(RawOps::default(), |total, stats| {
                total.add(RawOps::of(stats))
            });
        let raw_protocol = deltas
            .iter()
            .copied()
            .fold(RawProtocol::default(), |total, stats| {
                total.add(RawProtocol::of(stats))
            });
        let restructure = deltas
            .iter()
            .copied()
            .fold(metadata.restructure, Restructure::add_measured);
        let mut avoidable = glassdb::AvoidableTimeStats::default();
        for delta in deltas {
            avoidable += delta.restructurer.avoidable;
        }

        Self {
            mode: metadata.mode.to_string(),
            affinity_pct: metadata.affinity_pct,
            database_limit: metadata.database_limit,
            databases: metadata.databases,
            workers_per_shape: metadata.workers_per_shape,
            setup_splits: metadata.setup_splits,
            split_settle_wall_ms: metadata
                .split_settle_elapsed
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            failures: 0,
            policy: metadata.policy,
            restructure,
            shapes,
            aggregate_ops: raw_ops.per_tx(logical_txn),
            aggregate_protocol: raw_protocol.per_tx(logical_txn),
            avoidable_per_tx: AvoidablePerTx::of(avoidable, logical_txn),
        }
    }
}

/// Formats the progress line emitted before a cell starts.
pub(super) fn cell_started(run: usize, dimension: CellDimension) -> String {
    format!(
        "mixed: run={run} mode={} affinity={}% databases={}/{} workers/shape={}",
        dimension.mode.label(),
        dimension.affinity_pct,
        dimension.databases,
        dimension.database_limit,
        dimension.workers_per_shape,
    )
}

/// Formats the warning emitted when a cell misses its confidence target.
pub(super) fn cell_capped(dimension: CellDimension, target_ci: f64) -> String {
    format!(
        "  note: mode={} affinity={}% databases={}/{} workers/shape={} hit --max-duration before \
         every shape reached --target-ci={target_ci}",
        dimension.mode.label(),
        dimension.affinity_pct,
        dimension.databases,
        dimension.database_limit,
        dimension.workers_per_shape,
    )
}

/// Formats the setup-settlement summary emitted before measurement.
pub(super) fn setup_settled(elapsed: Duration, completed: u64, quiet: Duration) -> String {
    format!("  setup settled after {elapsed:?}: {completed} completed splits, {quiet:?} quiet")
}

/// Backend-op counters normalized per transaction.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpsPerTx {
    /// Successfully completed logical operations used as the denominator.
    txn: u64,
    /// Physical `Database::tx` calls, including unknown-outcome replays.
    attempted_txn: u64,
    obj_reads_per_tx: f64,
    obj_writes_per_tx: f64,
    obj_lists_per_tx: f64,
    total_ops_per_tx: f64,
    replays_per_tx: f64,
}

/// Coordinator and direct-path counters for the whole mixed cell.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtocolPerTx {
    coordinator_submissions_per_tx: f64,
    coordinator_rounds_per_tx: f64,
    coordinator_members_per_round: f64,
    coordinator_cas_retries_per_tx: f64,
    direct_candidates_per_tx: f64,
    direct_landed_per_tx: f64,
    direct_land_rate: f64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeResult {
    shape: String,
    committed: usize,
    tx_per_sec: f64,
    mean_ms: f64,
    p50_ms: f64,
    p90_ms: f64,
    /// Achieved relative half-width of the throughput 95% CI (`z/sqrt(committed)`,
    /// Poisson approximation); smaller is tighter.
    rel_ci: f64,
    /// Whether `rel_ci` met the run's `--target-ci`. `false` means the cell hit
    /// `--max-duration` first, so read this shape's throughput as indicative.
    converged: bool,
}

impl ShapeResult {
    fn summarize(measurement: ShapeMeasurement, target: u64) -> Self {
        let count = measurement.results.samples.len();
        let secs = measurement.results.tot_duration.as_secs_f64();
        let (p50_ms, p90_ms) = if count > 0 {
            (
                measurement.results.percentile(0.5).as_secs_f64() * 1000.0,
                measurement.results.percentile(0.9).as_secs_f64() * 1000.0,
            )
        } else {
            (0.0, 0.0)
        };

        Self {
            shape: measurement.shape.to_string(),
            committed: count,
            tx_per_sec: if secs > 0.0 { count as f64 / secs } else { 0.0 },
            mean_ms: measurement.results.avg().as_secs_f64() * 1000.0,
            p50_ms,
            p90_ms,
            rel_ci: measurement.results.rate_rel_ci(),
            converged: target == 0 || count as u64 >= target,
        }
    }
}

/// Raw backend and transaction deltas summed across a cell's Databases.
#[derive(Default, Clone, Copy)]
struct RawOps {
    reads: u64,
    writes: u64,
    lists: u64,
    txn: u64,
    replays: u64,
}

impl RawOps {
    fn of(delta: Stats) -> Self {
        Self {
            reads: delta.backend.obj_reads,
            writes: delta.backend.obj_writes,
            lists: delta.backend.obj_lists,
            txn: delta.transactions.completed,
            replays: delta.transactions.replays,
        }
    }

    fn add(self, other: Self) -> Self {
        Self {
            reads: self.reads + other.reads,
            writes: self.writes + other.writes,
            lists: self.lists + other.lists,
            txn: self.txn + other.txn,
            replays: self.replays + other.replays,
        }
    }

    fn per_tx(self, logical_txn: u64) -> OpsPerTx {
        let denominator = logical_txn.max(1) as f64;
        OpsPerTx {
            txn: logical_txn,
            attempted_txn: self.txn,
            obj_reads_per_tx: self.reads as f64 / denominator,
            obj_writes_per_tx: self.writes as f64 / denominator,
            obj_lists_per_tx: self.lists as f64 / denominator,
            total_ops_per_tx: (self.reads + self.writes + self.lists) as f64 / denominator,
            replays_per_tx: self.replays as f64 / denominator,
        }
    }
}

/// Raw protocol counter deltas summed across a cell's Databases.
#[derive(Default, Clone, Copy)]
struct RawProtocol {
    coordinator_submissions: u64,
    coordinator_rounds: u64,
    coordinator_cas_retries: u64,
    direct_candidates: u64,
    direct_landed: u64,
}

impl RawProtocol {
    fn of(delta: Stats) -> Self {
        Self {
            coordinator_submissions: delta.coordinator.submissions,
            coordinator_rounds: delta.coordinator.rounds,
            coordinator_cas_retries: delta.coordinator.cas_retries,
            direct_candidates: delta.direct_commit.candidates,
            direct_landed: delta.direct_commit.landed,
        }
    }

    fn add(self, other: Self) -> Self {
        Self {
            coordinator_submissions: self.coordinator_submissions + other.coordinator_submissions,
            coordinator_rounds: self.coordinator_rounds + other.coordinator_rounds,
            coordinator_cas_retries: self.coordinator_cas_retries + other.coordinator_cas_retries,
            direct_candidates: self.direct_candidates + other.direct_candidates,
            direct_landed: self.direct_landed + other.direct_landed,
        }
    }

    fn per_tx(self, logical_txn: u64) -> ProtocolPerTx {
        let denominator = logical_txn.max(1) as f64;
        ProtocolPerTx {
            coordinator_submissions_per_tx: self.coordinator_submissions as f64 / denominator,
            coordinator_rounds_per_tx: self.coordinator_rounds as f64 / denominator,
            coordinator_members_per_round: ratio(
                self.coordinator_submissions,
                self.coordinator_rounds,
            ),
            coordinator_cas_retries_per_tx: self.coordinator_cas_retries as f64 / denominator,
            direct_candidates_per_tx: self.direct_candidates as f64 / denominator,
            direct_landed_per_tx: self.direct_landed as f64 / denominator,
            direct_land_rate: ratio(self.direct_landed, self.direct_candidates),
        }
    }
}

impl AvoidablePerTx {
    fn of(avoidable: glassdb::AvoidableTimeStats, logical_txn: u64) -> Self {
        let per_tx = |time: Duration| time.as_secs_f64() * 1e3 / logical_txn.max(1) as f64;
        Self {
            lost_cas_ms: per_tx(avoidable.split.lost_cas),
            queue_wait_ms: per_tx(avoidable.split.queue_wait),
            slow_cas_ms: per_tx(avoidable.split.slow_cas),
            inline_pressure_ms: per_tx(avoidable.split.inline_pressure),
            adjacent_miss_ms: per_tx(avoidable.merge.adjacent_miss),
            scan_crossing_ms: per_tx(avoidable.merge.scan_crossing),
        }
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use glassdb::{
        AvoidableTimeStats, BackendStats, DirectCommitStats, LeafCoordinatorStats,
        RestructurerStats, SplitTime, TransactionStats,
    };

    use super::*;

    #[test]
    fn fixed_samples_preserve_serialized_metrics() {
        let measurements = vec![
            ShapeMeasurement::new(
                "rwSingle",
                Results {
                    samples: vec![
                        Duration::from_millis(10),
                        Duration::from_millis(20),
                        Duration::from_millis(30),
                    ],
                    tot_duration: Duration::from_secs(2),
                },
            ),
            ShapeMeasurement::new("roMulti", Results::default()),
        ];
        let deltas = [
            Stats {
                transactions: TransactionStats {
                    completed: 3,
                    replays: 2,
                    ..Default::default()
                },
                backend: BackendStats {
                    obj_reads: 8,
                    obj_writes: 4,
                    obj_lists: 0,
                },
                coordinator: LeafCoordinatorStats {
                    submissions: 12,
                    rounds: 4,
                    cas_retries: 2,
                    ..Default::default()
                },
                direct_commit: DirectCommitStats {
                    candidates: 5,
                    landed: 3,
                    ..Default::default()
                },
                restructurer: RestructurerStats {
                    splits: 1,
                    merges: 2,
                    avoidable: AvoidableTimeStats {
                        split: SplitTime {
                            lost_cas: Duration::from_millis(6),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            Stats {
                transactions: TransactionStats {
                    completed: 2,
                    replays: 1,
                    ..Default::default()
                },
                backend: BackendStats {
                    obj_reads: 1,
                    obj_writes: 2,
                    obj_lists: 2,
                },
                coordinator: LeafCoordinatorStats {
                    submissions: 3,
                    rounds: 1,
                    ..Default::default()
                },
                direct_commit: DirectCommitStats {
                    candidates: 1,
                    landed: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        ];
        let mut warmup = Restructure::default();
        warmup.add_warmup(Stats {
            restructurer: RestructurerStats {
                splits: 5,
                ..Default::default()
            },
            ..Default::default()
        });
        let cell = CellResult::summarize(
            CellMetadata::new("hi", 25, 2, 2, 8, 4, Duration::from_millis(1500))
                .with_policy("avoidable".into(), warmup),
            measurements,
            &deltas,
            3,
        );
        let empty_cell = CellResult::summarize(
            CellMetadata::new("lo", 0, 0, 0, 0, 0, Duration::ZERO),
            std::iter::empty(),
            &[],
            3,
        );
        let report = RunResult::new(7, vec![cell, empty_cell]);

        assert_eq!(
            serde_json::to_string_pretty(&serde_json::to_value(&report).unwrap()).unwrap(),
            r#"{
  "cells": [
    {
      "affinityPct": 25,
      "aggregateOps": {
        "attemptedTxn": 5,
        "objListsPerTx": 0.6666666666666666,
        "objReadsPerTx": 3.0,
        "objWritesPerTx": 2.0,
        "replaysPerTx": 1.0,
        "totalOpsPerTx": 5.666666666666667,
        "txn": 3
      },
      "aggregateProtocol": {
        "coordinatorCasRetriesPerTx": 0.6666666666666666,
        "coordinatorMembersPerRound": 3.0,
        "coordinatorRoundsPerTx": 1.6666666666666667,
        "coordinatorSubmissionsPerTx": 5.0,
        "directCandidatesPerTx": 2.0,
        "directLandRate": 0.6666666666666666,
        "directLandedPerTx": 1.3333333333333333
      },
      "avoidablePerTx": {
        "adjacentMissMs": 0.0,
        "inlinePressureMs": 0.0,
        "lostCasMs": 2.0,
        "queueWaitMs": 0.0,
        "scanCrossingMs": 0.0,
        "slowCasMs": 0.0
      },
      "databaseLimit": 2,
      "databases": 2,
      "failures": 0,
      "mode": "hi",
      "policy": "avoidable",
      "restructure": {
        "measuredMerges": 2,
        "measuredSplits": 1,
        "warmupMerges": 0,
        "warmupSplits": 5
      },
      "setupSplits": 4,
      "shapes": [
        {
          "committed": 3,
          "converged": true,
          "meanMs": 20.0,
          "p50Ms": 20.0,
          "p90Ms": 30.0,
          "relCi": 1.1316065276116665,
          "shape": "rwSingle",
          "txPerSec": 1.5
        },
        {
          "committed": 0,
          "converged": false,
          "meanMs": 0.0,
          "p50Ms": 0.0,
          "p90Ms": 0.0,
          "relCi": 99.0,
          "shape": "roMulti",
          "txPerSec": 0.0
        }
      ],
      "splitSettleWallMs": 1500,
      "workersPerShape": 8
    },
    {
      "affinityPct": 0,
      "aggregateOps": {
        "attemptedTxn": 0,
        "objListsPerTx": 0.0,
        "objReadsPerTx": 0.0,
        "objWritesPerTx": 0.0,
        "replaysPerTx": 0.0,
        "totalOpsPerTx": 0.0,
        "txn": 0
      },
      "aggregateProtocol": {
        "coordinatorCasRetriesPerTx": 0.0,
        "coordinatorMembersPerRound": 0.0,
        "coordinatorRoundsPerTx": 0.0,
        "coordinatorSubmissionsPerTx": 0.0,
        "directCandidatesPerTx": 0.0,
        "directLandRate": 0.0,
        "directLandedPerTx": 0.0
      },
      "avoidablePerTx": {
        "adjacentMissMs": 0.0,
        "inlinePressureMs": 0.0,
        "lostCasMs": 0.0,
        "queueWaitMs": 0.0,
        "scanCrossingMs": 0.0,
        "slowCasMs": 0.0
      },
      "databaseLimit": 0,
      "databases": 0,
      "failures": 0,
      "mode": "lo",
      "policy": "engine",
      "restructure": {
        "measuredMerges": 0,
        "measuredSplits": 0,
        "warmupMerges": 0,
        "warmupSplits": 0
      },
      "setupSplits": 0,
      "shapes": [],
      "splitSettleWallMs": 0,
      "workersPerShape": 0
    }
  ],
  "run": 7
}"#
        );
    }
}
