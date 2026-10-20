use std::time::Duration;

use serde::Serialize;

use glassdb::{GcStats, Stats};
use glassdb_bench_scale::bench::Results;

use super::options::CellDimension;
use super::setup::{Collected, Warmup};
use super::workload::KeySelection;

/// Number of equal windows that split each cell's measured interval. Their
/// commit counts give each shape's throughput distribution. With 20 windows,
/// the 90th percentile does not depend on only one or two windows.
///
/// All workers start the measurement with no transaction in flight, so the
/// first window usually has fewer commits than the others. The p50 and p90 of
/// the windows do not change much because of one low window.
pub(super) const THROUGHPUT_WINDOWS: usize = 20;

/// Two-sided 95% Student-t quantile for the batch means of the windows.
const T_95_WINDOWS: f64 = t_95(THROUGHPUT_WINDOWS - 1);
// Below 4 degrees of freedom, `t_95` is more than 0.03% off.
const _: () = assert!(THROUGHPUT_WINDOWS >= 5);

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
    shared_pct: Option<u8>,
    warmup: Option<Warmup>,
    collected: Option<Collected>,
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
            shared_pct: None,
            warmup: None,
            collected: None,
        }
    }

    /// Records the cell's warm-up before the measurement.
    pub(super) fn with_warmup(mut self, warmup: Warmup) -> Self {
        self.warmup = Some(warmup);
        self
    }

    /// Records the full GCs between the warm-up and the measurement, when
    /// they completed.
    pub(super) fn with_collected(mut self, collected: Option<Collected>) -> Self {
        self.collected = collected;
        self
    }

    /// Records how workers chose keys in the cell.
    pub(super) fn with_key_selection(mut self, keys: KeySelection) -> Self {
        self.shared_pct = match keys {
            KeySelection::Shared => None,
            KeySelection::Stripes { shared_pct } => Some(shared_pct),
        };
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
    /// Percentage of transactions that draw keys outside their worker's
    /// stripe. Absent when workers do not use stripes.
    #[serde(skip_serializing_if = "Option::is_none")]
    shared_pct: Option<u8>,
    setup_splits: u64,
    split_settle_wall_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    warmup: Option<WarmupResult>,
    /// The full GCs between the warm-up and the measurement, which show the
    /// garbage that background GC left after the warm-up.
    #[serde(skip_serializing_if = "Option::is_none")]
    full_gc: Option<FullGcResult>,
    failures: u64,
    shapes: Vec<ShapeResult>,
    aggregate_ops: OpsPerTx,
    aggregate_protocol: ProtocolPerTx,
    /// The activity of background GC during the measurement.
    gc: GcResult,
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

        Self {
            mode: metadata.mode.to_string(),
            affinity_pct: metadata.affinity_pct,
            database_limit: metadata.database_limit,
            databases: metadata.databases,
            workers_per_shape: metadata.workers_per_shape,
            shared_pct: metadata.shared_pct,
            setup_splits: metadata.setup_splits,
            split_settle_wall_ms: metadata
                .split_settle_elapsed
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            warmup: metadata.warmup.map(WarmupResult::from),
            full_gc: metadata.collected.map(FullGcResult::from),
            failures: 0,
            shapes,
            aggregate_ops: raw_ops.per_tx(logical_txn),
            aggregate_protocol: raw_protocol.per_tx(logical_txn),
            gc: GcResult::from(deltas.iter().fold(GcStats::default(), |mut total, stats| {
                total += stats.gc;
                total
            })),
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

/// Formats the warm-up summary emitted after a cell.
pub(super) fn cell_warmed_up(warmup: &Warmup) -> String {
    format!(
        "  warm-up took {:?}: {} splits and {} merges",
        warmup.elapsed, warmup.splits, warmup.merges
    )
}

/// Formats the warning emitted when a cell's warm-up does not become quiet.
pub(super) fn warmup_capped(dimension: CellDimension, timeout: Duration) -> String {
    format!(
        "  note: mode={} affinity={}% databases={}/{} workers/shape={} measured after \
         --warmup-timeout={timeout:?} with splits or merges still in progress",
        dimension.mode.label(),
        dimension.affinity_pct,
        dimension.databases,
        dimension.database_limit,
        dimension.workers_per_shape,
    )
}

/// Formats the warning emitted when a cell misses its sampling target.
pub(super) fn cell_capped(dimension: CellDimension, window_commits: u64) -> String {
    format!(
        "  note: mode={} affinity={}% databases={}/{} workers/shape={} hit --max-duration before \
         every shape reached --window-commits={window_commits}",
        dimension.mode.label(),
        dimension.affinity_pct,
        dimension.databases,
        dimension.database_limit,
        dimension.workers_per_shape,
    )
}

/// Formats the full GCs that ran after a load.
pub(super) fn garbage_collected(collected: &Collected) -> String {
    format!(
        "  full GC took {:?}: {} transaction records deleted",
        collected.elapsed, collected.deleted
    )
}

/// Formats the setup-settlement summary emitted before measurement.
pub(super) fn setup_settled(elapsed: Duration, completed: u64, quiet: Duration) -> String {
    format!("  setup settled after {elapsed:?}: {completed} completed splits, {quiet:?} quiet")
}

/// The structural changes and wall time of a cell's warm-up.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct WarmupResult {
    /// Splits and merges of the measurement database instances during the
    /// warm-up.
    splits: u64,
    merges: u64,
    wall_ms: u64,
    /// Whether the splits and merges stopped before the warm-up timeout.
    quiet: bool,
    /// The activity of background GC during the warm-up.
    gc: GcResult,
}

impl From<Warmup> for WarmupResult {
    fn from(warmup: Warmup) -> Self {
        Self {
            splits: warmup.splits,
            merges: warmup.merges,
            wall_ms: warmup.elapsed.as_millis().try_into().unwrap_or(u64::MAX),
            quiet: warmup.quiet,
            gc: GcResult::from(warmup.gc),
        }
    }
}

/// The GC counters of the database instances over one interval.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct GcResult {
    /// Completed GC checks, and those that found their record inside its
    /// retention period.
    checks: u64,
    deferred_checks: u64,
    /// Checks that reclaimed effects or deleted their record.
    progress: u64,
    /// The estimated writes of GC.
    writes: u64,
    lists: u64,
    dropped_candidates: u64,
    failures: u64,
}

impl From<GcStats> for GcResult {
    fn from(gc: GcStats) -> Self {
        Self {
            checks: gc.checks,
            deferred_checks: gc.deferred_checks,
            progress: gc.progress,
            writes: gc.writes,
            lists: gc.lists,
            dropped_candidates: gc.dropped_candidates,
            failures: gc.failures,
        }
    }
}

/// The full GCs between the warm-up and the measurement.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct FullGcResult {
    deleted: u64,
    wall_ms: u64,
}

impl From<Collected> for FullGcResult {
    fn from(collected: Collected) -> Self {
        Self {
            deleted: collected.deleted,
            wall_ms: collected.elapsed.as_millis().try_into().unwrap_or(u64::MAX),
        }
    }
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
    /// Transactions with a latency sample, including the ones that complete
    /// while the workers drain after the measured interval.
    committed: usize,
    /// Commits in the throughput windows per second of the measured interval.
    tx_per_sec: f64,
    mean_ms: f64,
    p50_ms: f64,
    p90_ms: f64,
    /// Relative half-width of the throughput 95% CI, from the batch means of
    /// the windows. Unlike a Poisson bound, it includes stalls and correlated
    /// commits. Smaller is tighter.
    rel_ci: f64,
    /// Model-time width of each throughput window.
    window_ms: f64,
    /// Commits in each of the equal windows of the measured interval.
    window_counts: Vec<u64>,
    /// Whether the windows reached the run's `--window-commits`. `false` means
    /// the cell hit `--max-duration` first, so read this shape's throughput as
    /// indicative and its distribution as noisier.
    converged: bool,
}

impl ShapeResult {
    fn summarize(measurement: ShapeMeasurement, target: u64) -> Self {
        let count = measurement.results.samples.len();
        let duration = measurement.results.tot_duration;
        let window_counts = measurement.results.window_counts(THROUGHPUT_WINDOWS);
        let windowed: u64 = window_counts.iter().sum();
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
            tx_per_sec: if duration.is_zero() {
                0.0
            } else {
                windowed as f64 / duration.as_secs_f64()
            },
            mean_ms: measurement.results.avg().as_secs_f64() * 1000.0,
            p50_ms,
            p90_ms,
            rel_ci: batch_means_rel_ci(&window_counts),
            window_ms: duration.as_secs_f64() * 1000.0 / THROUGHPUT_WINDOWS as f64,
            converged: target == 0 || windowed >= target,
            window_counts,
        }
    }
}

/// Returns the two-sided 95% Student-t quantile for `degrees` of freedom.
///
/// This is the Cornish-Fisher expansion of Abramowitz and Stegun (26.7.5).
/// The crate has no statistics dependency, and the expansion lets the
/// quantile follow `THROUGHPUT_WINDOWS` at compile time.
const fn t_95(degrees: usize) -> f64 {
    // The two-sided 95% quantile of the standard normal distribution.
    const Z: f64 = 1.959_963_984_540_054;
    let v = degrees as f64;
    let z2 = Z * Z;
    let g1 = (z2 + 1.0) / 4.0;
    let g2 = (5.0 * z2 * z2 + 16.0 * z2 + 3.0) / 96.0;
    let g3 = (3.0 * z2 * z2 * z2 + 19.0 * z2 * z2 + 17.0 * z2 - 15.0) / 384.0;
    let g4 =
        (79.0 * z2 * z2 * z2 * z2 + 776.0 * z2 * z2 * z2 + 1482.0 * z2 * z2 - 1920.0 * z2 - 945.0)
            / 92160.0;
    Z * (1.0 + g1 / v + g2 / (v * v) + g3 / (v * v * v) + g4 / (v * v * v * v))
}

/// Returns the relative half-width of the 95% CI of the mean window count.
///
/// Returns a large finite sentinel when no window has a commit, so that
/// callers can serialize it.
fn batch_means_rel_ci(counts: &[u64]) -> f64 {
    let n = counts.len() as f64;
    let mean = counts.iter().sum::<u64>() as f64 / n;
    if counts.len() < 2 || mean == 0.0 {
        return 99.0;
    }
    let variance = counts
        .iter()
        .map(|&count| (count as f64 - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    T_95_WINDOWS * variance.sqrt() / (mean * n.sqrt())
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

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use glassdb::{BackendStats, DirectCommitStats, LeafCoordinatorStats, TransactionStats};

    use super::*;

    #[test]
    fn t_95_matches_student_t_tables() {
        // Exact quantiles from statistical tables, to five decimals.
        for (degrees, expected) in [
            (4, 2.77645),
            (9, 2.26216),
            (19, 2.09302),
            (29, 2.04523),
            (99, 1.98422),
        ] {
            let actual = t_95(degrees);
            assert!(
                (actual - expected).abs() / expected < 3.5e-4,
                "t_95({degrees}) = {actual}, expected {expected}"
            );
        }
    }

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
                    // The last transaction completes while the workers drain.
                    completions: [50, 1050, 2100].map(Duration::from_millis).to_vec(),
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
                },
                direct_commit: DirectCommitStats {
                    candidates: 5,
                    landed: 3,
                },
                gc: GcStats {
                    checks: 4,
                    deferred_checks: 1,
                    progress: 2,
                    writes: 3,
                    lists: 1,
                    dropped_candidates: 5,
                    failures: 0,
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
                    cas_retries: 0,
                },
                direct_commit: DirectCommitStats {
                    candidates: 1,
                    landed: 1,
                },
                ..Default::default()
            },
        ];
        let cell = CellResult::summarize(
            CellMetadata::new("hi", 25, 2, 2, 8, 4, Duration::from_millis(1500)),
            measurements,
            &deltas,
            2,
        );
        let empty_cell = CellResult::summarize(
            CellMetadata::new("lo", 0, 0, 0, 0, 0, Duration::ZERO)
                .with_key_selection(KeySelection::Stripes { shared_pct: 1 })
                .with_warmup(Warmup {
                    splits: 5,
                    merges: 2,
                    elapsed: Duration::from_millis(2500),
                    quiet: false,
                    gc: GcStats {
                        checks: 10,
                        deferred_checks: 3,
                        progress: 6,
                        writes: 7,
                        lists: 2,
                        ..GcStats::default()
                    },
                })
                .with_collected(Some(Collected {
                    deleted: 40,
                    elapsed: Duration::from_millis(1200),
                })),
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
      "databaseLimit": 2,
      "databases": 2,
      "failures": 0,
      "gc": {
        "checks": 4,
        "deferredChecks": 1,
        "droppedCandidates": 5,
        "failures": 0,
        "lists": 1,
        "progress": 2,
        "writes": 3
      },
      "mode": "hi",
      "setupSplits": 4,
      "shapes": [
        {
          "committed": 3,
          "converged": true,
          "meanMs": 20.0,
          "p50Ms": 20.0,
          "p90Ms": 30.0,
          "relCi": 1.4405177354239902,
          "shape": "rwSingle",
          "txPerSec": 1.0,
          "windowCounts": [
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0
          ],
          "windowMs": 100.0
        },
        {
          "committed": 0,
          "converged": false,
          "meanMs": 0.0,
          "p50Ms": 0.0,
          "p90Ms": 0.0,
          "relCi": 99.0,
          "shape": "roMulti",
          "txPerSec": 0.0,
          "windowCounts": [],
          "windowMs": 0.0
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
      "databaseLimit": 0,
      "databases": 0,
      "failures": 0,
      "fullGc": {
        "deleted": 40,
        "wallMs": 1200
      },
      "gc": {
        "checks": 0,
        "deferredChecks": 0,
        "droppedCandidates": 0,
        "failures": 0,
        "lists": 0,
        "progress": 0,
        "writes": 0
      },
      "mode": "lo",
      "setupSplits": 0,
      "shapes": [],
      "sharedPct": 1,
      "splitSettleWallMs": 0,
      "warmup": {
        "gc": {
          "checks": 10,
          "deferredChecks": 3,
          "droppedCandidates": 0,
          "failures": 0,
          "lists": 2,
          "progress": 6,
          "writes": 7
        },
        "merges": 2,
        "quiet": false,
        "splits": 5,
        "wallMs": 2500
      },
      "workersPerShape": 0
    }
  ],
  "run": 7
}"#
        );
    }
}
