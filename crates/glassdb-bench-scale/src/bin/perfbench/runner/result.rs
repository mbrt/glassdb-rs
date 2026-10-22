//! The result of a cell, in the fields that the comparison and plot tools
//! read.

use glassdb::{GcStats, Stats};
use glassdb_bench_scale::backend_breakdown::BackendBreakdown;
use glassdb_bench_scale::bench::Results;
use serde::Serialize;

use super::Window;
use super::seed::{Collected, SeedReport};

/// Number of equal windows that split each cell's measured interval. Their
/// commit counts give each shape's throughput distribution. With 20 windows,
/// the 90th percentile does not depend on only one or two windows.
///
/// All workers start the measurement with no transaction in flight, so the
/// first window usually has fewer commits than the others. The p50 and p90 of
/// the windows do not change much because of one low window.
pub(crate) const THROUGHPUT_WINDOWS: usize = 20;

/// Two-sided 95% Student-t quantile for the batch means of the windows.
const T_95_WINDOWS: f64 = t_95(THROUGHPUT_WINDOWS - 1);
// Below 4 degrees of freedom, `t_95` is more than 0.03% off.
const _: () = assert!(THROUGHPUT_WINDOWS >= 5);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunResult {
    pub(crate) run: usize,
    pub(crate) cells: Vec<CellResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CellResult {
    /// The label of each axis value of the cell.
    pub(crate) labels: serde_json::Map<String, serde_json::Value>,
    /// The values of the cell that its axes set, by dotted path, and a summary
    /// of its shapes: `keys` and `shared` of an overlap source, and
    /// `sharedPct` of the stripes.
    pub(crate) parameters: serde_json::Map<String, serde_json::Value>,
    /// Where the cell started: `seed`, `previous`, or `previous-warm-up`.
    pub(crate) start: &'static str,
    pub(crate) workload: String,
    pub(crate) affinity_pct: u8,
    /// `cell.databases` and `cell.workers`, when a group of the workload
    /// uses them; a workload with fixed instances and workers has neither.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) database_limit: Option<usize>,
    pub(crate) databases: usize,
    pub(crate) collections: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) workers_per_shape: Option<usize>,
    /// The failed transactions of the cell. A cell with failures ends with an
    /// error, so a written cell always has none; the readers check it.
    pub(crate) failures: u64,
    pub(crate) seed: SeedReport,
    pub(crate) warmup: WarmupResult,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) full_gc: Option<Collected>,
    pub(crate) measure: MeasureResult,
    pub(crate) shapes: Vec<ShapeResult>,
    pub(crate) groups: Vec<GroupResult>,
    pub(crate) aggregate_ops: OpsPerTx,
    pub(crate) aggregate_protocol: ProtocolPerTx,
    /// The activity of background GC during the measurement.
    pub(crate) gc: GcResult,
    pub(crate) timeline: Vec<TimelineEntry>,
    /// The backend requests, body bytes, and coordinator counters of each
    /// transaction.
    pub(crate) costs: serde_json::Value,
}

/// The structural changes and wall time of a cell's warm-up.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WarmupResult {
    pub(crate) splits: u64,
    pub(crate) merges: u64,
    pub(crate) wall_ms: u64,
    /// Whether the splits and merges stopped before the warm-up timeout.
    pub(crate) quiet: bool,
    pub(crate) gc: GcResult,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MeasureResult {
    pub(crate) wall_ms: u64,
    /// Whether the end condition was met before the maximum duration.
    pub(crate) met: bool,
    pub(crate) splits: u64,
    pub(crate) merges: u64,
    /// The wall time after the measurement until all workers stopped.
    pub(crate) worker_drain_ms: f64,
}

/// The GC counters of the database instances over one interval.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GcResult {
    checks: u64,
    deferred_checks: u64,
    progress: u64,
    writes: u64,
    lists: u64,
    dropped_candidates: u64,
    failures: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupResult {
    pub(crate) group: String,
    pub(crate) instances: Vec<InstanceResult>,
}

/// The counters of one database instance in the measurement.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InstanceResult {
    committed: u64,
    replays: u64,
    lock_calls: u64,
    direct_candidates: u64,
    direct_landed: u64,
    obj_reads: u64,
    obj_writes: u64,
    obj_lists: u64,
    coordinator_submissions: u64,
    coordinator_rounds: u64,
    coordinator_cas_retries: u64,
    split_candidates: u64,
    splits: u64,
    split_deferred: u64,
    stale_demand: u64,
    merges: u64,
}

/// Backend-op counters normalized per transaction.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OpsPerTx {
    /// Successfully completed logical operations used as the denominator.
    txn: u64,
    /// Physical `Database::tx` calls, including unknown-outcome replays.
    attempted_txn: u64,
    obj_reads_per_tx: f64,
    obj_writes_per_tx: f64,
    obj_lists_per_tx: f64,
    total_ops_per_tx: f64,
    replays_per_tx: f64,
    /// The backend operations and bytes of all object roles, as the storage
    /// counted them.
    backend_ops: u64,
    read_bytes: u64,
    write_bytes: u64,
}

/// Coordinator and direct-path counters for the whole cell.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ProtocolPerTx {
    coordinator_submissions_per_tx: f64,
    coordinator_rounds_per_tx: f64,
    coordinator_members_per_round: f64,
    coordinator_cas_retries_per_tx: f64,
    direct_candidates_per_tx: f64,
    direct_landed_per_tx: f64,
    direct_land_rate: f64,
    lock_calls: u64,
    replays: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShapeResult {
    pub(crate) shape: String,
    pub(crate) group: String,
    pub(crate) workers: usize,
    /// Transactions with a latency sample, including the ones that complete
    /// while the workers drain after the measured interval.
    pub(crate) committed: usize,
    /// Commits in the throughput windows per second of the measured interval.
    pub(crate) tx_per_sec: f64,
    pub(crate) mean_ms: f64,
    /// The standard error of the mean latency.
    pub(crate) mean_se_ms: f64,
    pub(crate) p50_ms: f64,
    pub(crate) p90_ms: f64,
    /// Relative half-width of the throughput 95% CI, from the batch means of
    /// the windows. Unlike a Poisson bound, it includes stalls and correlated
    /// commits. Smaller is tighter.
    pub(crate) rel_ci: f64,
    /// Model-time width of each throughput window.
    pub(crate) window_ms: f64,
    /// Commits in each of the equal windows of the measured interval.
    pub(crate) window_counts: Vec<u64>,
    /// Whether the windows reached the commits of the end condition. `false`
    /// means the cell hit its maximum duration first, so read this shape's
    /// throughput as indicative and its distribution as noisier.
    pub(crate) converged: bool,
}

/// The structural changes of each instance in one wall second.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TimelineEntry {
    pub(crate) second: u64,
    /// Whether the second is in the measurement, and not in the warm-up.
    pub(crate) measured: bool,
    /// The splits and merges of each instance, in the order of the groups.
    pub(crate) splits: Vec<u64>,
    pub(crate) merges: Vec<u64>,
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

impl InstanceResult {
    pub(crate) fn of(stats: &Stats) -> Self {
        Self {
            committed: stats.transactions.completed,
            replays: stats.transactions.replays,
            lock_calls: stats.locker.calls,
            direct_candidates: stats.direct_commit.candidates,
            direct_landed: stats.direct_commit.landed,
            obj_reads: stats.backend.obj_reads,
            obj_writes: stats.backend.obj_writes,
            obj_lists: stats.backend.obj_lists,
            coordinator_submissions: stats.coordinator.submissions,
            coordinator_rounds: stats.coordinator.rounds,
            coordinator_cas_retries: stats.coordinator.cas_retries,
            split_candidates: stats.restructurer.candidates,
            splits: stats.restructurer.splits,
            split_deferred: stats.restructurer.deferred,
            stale_demand: stats.restructurer.stale_demand,
            merges: stats.restructurer.merges,
        }
    }
}

impl OpsPerTx {
    pub(crate) fn of(total: &Stats, logical_txn: u64, backend: BackendBreakdown) -> Self {
        let denominator = logical_txn.max(1) as f64;
        let (reads, writes, lists) = (
            total.backend.obj_reads,
            total.backend.obj_writes,
            total.backend.obj_lists,
        );
        Self {
            txn: logical_txn,
            attempted_txn: total.transactions.completed,
            obj_reads_per_tx: reads as f64 / denominator,
            obj_writes_per_tx: writes as f64 / denominator,
            obj_lists_per_tx: lists as f64 / denominator,
            total_ops_per_tx: (reads + writes + lists) as f64 / denominator,
            replays_per_tx: total.transactions.replays as f64 / denominator,
            backend_ops: backend.total(),
            read_bytes: backend.read_bytes(),
            write_bytes: backend.write_bytes(),
        }
    }
}

impl ProtocolPerTx {
    pub(crate) fn of(total: &Stats, logical_txn: u64) -> Self {
        let denominator = logical_txn.max(1) as f64;
        let rounds = total.coordinator.rounds;
        let candidates = total.direct_commit.candidates;
        Self {
            coordinator_submissions_per_tx: total.coordinator.submissions as f64 / denominator,
            coordinator_rounds_per_tx: rounds as f64 / denominator,
            coordinator_members_per_round: if rounds == 0 {
                0.0
            } else {
                total.coordinator.submissions as f64 / rounds as f64
            },
            coordinator_cas_retries_per_tx: total.coordinator.cas_retries as f64 / denominator,
            direct_candidates_per_tx: candidates as f64 / denominator,
            direct_landed_per_tx: total.direct_commit.landed as f64 / denominator,
            direct_land_rate: if candidates == 0 {
                0.0
            } else {
                total.direct_commit.landed as f64 / candidates as f64
            },
            lock_calls: total.locker.calls,
            replays: total.transactions.replays,
        }
    }
}

impl ShapeResult {
    /// Summarizes the samples of one shape. `target` is the commits that the
    /// windows must reach, or zero when any count converges.
    pub(crate) fn summarize(
        shape: &str,
        group: &str,
        workers: usize,
        results: &Results,
        target: u64,
    ) -> Self {
        let count = results.samples.len();
        let duration = results.tot_duration;
        let window_counts = results.window_counts(THROUGHPUT_WINDOWS);
        let windowed: u64 = window_counts.iter().sum();
        let percentile = |p| {
            if count == 0 {
                0.0
            } else {
                results.percentile(p).as_secs_f64() * 1000.0
            }
        };
        Self {
            shape: shape.to_string(),
            group: group.to_string(),
            workers,
            committed: count,
            tx_per_sec: if duration.is_zero() {
                0.0
            } else {
                windowed as f64 / duration.as_secs_f64()
            },
            mean_ms: results.avg().as_secs_f64() * 1000.0,
            mean_se_ms: standard_error_ms(results),
            p50_ms: percentile(0.5),
            p90_ms: percentile(0.9),
            rel_ci: batch_means_rel_ci(&window_counts),
            window_ms: duration.as_secs_f64() * 1000.0 / THROUGHPUT_WINDOWS as f64,
            converged: target == 0 || windowed >= target,
            window_counts,
        }
    }
}

/// Returns the backend requests, the body bytes, and the coordinator counters
/// of each of `transactions`, in the windows of the workload, of the shutdown
/// of the instances, and of both.
pub(crate) fn costs(workload: Window, shutdown: Window, transactions: u64) -> serde_json::Value {
    let n = transactions.max(1) as f64;
    let window = |window: &Window| {
        serde_json::json!({
            "reads": window.stats.backend.obj_reads as f64 / n,
            "writes": window.stats.backend.obj_writes as f64 / n,
            "lists": window.stats.backend.obj_lists as f64 / n,
            "readBodyBytes": window.breakdown.read_bytes() as f64 / n,
            "writeBodyBytes": window.breakdown.write_bytes() as f64 / n,
            "coordinatorSubmissions": window.stats.coordinator.submissions as f64 / n,
            "coordinatorRounds": window.stats.coordinator.rounds as f64 / n,
        })
    };
    let mut combined = workload;
    combined.add(shutdown);
    serde_json::json!({
        "transactions": transactions,
        "workload": window(&workload),
        "shutdown": window(&shutdown),
        "combined": window(&combined),
    })
}

/// Returns the standard error of the mean of the samples, in milliseconds.
fn standard_error_ms(results: &Results) -> f64 {
    let n = results.samples.len();
    if n < 2 {
        return 0.0;
    }
    let mean = results.avg().as_secs_f64();
    let variance = results
        .samples
        .iter()
        .map(|sample| (sample.as_secs_f64() - mean).powi(2))
        .sum::<f64>()
        / (n - 1) as f64;
    (variance / n as f64).sqrt() * 1000.0
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn t_quantile_matches_published_values() {
        // Two-sided 95% quantiles of Student's t distribution.
        for (degrees, expected) in [(4, 2.776_445), (9, 2.262_157), (19, 2.093_024)] {
            assert!((t_95(degrees) - expected).abs() < 1e-3, "{degrees}");
        }
    }

    #[test]
    fn equal_windows_have_a_zero_interval_and_empty_ones_a_sentinel() {
        assert_eq!(batch_means_rel_ci(&[5; THROUGHPUT_WINDOWS]), 0.0);
        assert_eq!(batch_means_rel_ci(&[0; THROUGHPUT_WINDOWS]), 99.0);
    }

    #[test]
    fn a_shape_converges_when_its_windows_reach_the_target() {
        let results = Results {
            samples: vec![Duration::from_millis(10); 4],
            completions: (0..4).map(|i| Duration::from_millis(100 * i)).collect(),
            tot_duration: Duration::from_secs(1),
        };
        let shape = ShapeResult::summarize("s", "g", 1, &results, 4);
        assert!(shape.converged);
        assert_eq!(shape.committed, 4);
        assert_eq!(shape.mean_se_ms, 0.0);
        assert!(!ShapeResult::summarize("s", "g", 1, &results, 5).converged);
    }
}
