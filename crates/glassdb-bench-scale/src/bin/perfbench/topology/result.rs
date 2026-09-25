//! Serialized topology scenario reports.

use std::time::Duration;

use serde::Serialize;

use glassdb::Stats;
use glassdb_bench_scale::bench::Results;
use glassdb_bench_scale::run::SplitSettlement;

use super::Cell;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RunResult {
    run: usize,
    topologies: Vec<TopologyResult>,
    cells: Vec<CellResult>,
}

impl RunResult {
    pub(super) fn new(run: usize, topologies: Vec<TopologyResult>, cells: Vec<CellResult>) -> Self {
        Self {
            run,
            topologies,
            cells,
        }
    }
}

/// The seeded tree shape that every cell of one leaf size shares.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TopologyResult {
    leaf_max_entries: usize,
    keys: usize,
    /// Standalone tree nodes: every node except the tree roots.
    nodes: usize,
    setup_splits: u64,
    split_settle_wall_ms: u64,
}

impl TopologyResult {
    pub(super) fn new(
        leaf_max_entries: usize,
        keys: usize,
        nodes: usize,
        settlement: SplitSettlement,
    ) -> Self {
        Self {
            leaf_max_entries,
            keys,
            nodes,
            setup_splits: settlement.completed,
            split_settle_wall_ms: settlement
                .elapsed
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        }
    }

    /// Formats the setup summary emitted before measurement.
    pub(super) fn summary(&self) -> String {
        format!(
            "topology: leaf-size={} keys={} standalone-nodes={} after {} splits",
            self.leaf_max_entries, self.keys, self.nodes, self.setup_splits
        )
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CellResult {
    leaf_max_entries: usize,
    workload: &'static str,
    databases: usize,
    workers: usize,
    committed: usize,
    tx_per_sec: f64,
    mean_ms: f64,
    p50_ms: f64,
    p90_ms: f64,
    /// Achieved relative half-width of the throughput 95% CI.
    rel_ci: f64,
    /// Whether the cell reached `--target-ci` before `--max-duration`.
    converged: bool,
    /// Splits and merges during measurement. Nonzero values mean the cell did
    /// not measure the seeded topology alone.
    measured_splits: u64,
    measured_merges: u64,
    per_tx: PerTx,
}

impl CellResult {
    /// Summarizes the timing samples and the counter deltas of all clients.
    pub(super) fn summarize(
        cell: Cell,
        workers: usize,
        results: Results,
        converged: bool,
        delta: Stats,
    ) -> Self {
        let committed = results.samples.len();
        let secs = results.tot_duration.as_secs_f64();
        let (p50_ms, p90_ms) = if committed > 0 {
            (
                results.percentile(0.5).as_secs_f64() * 1000.0,
                results.percentile(0.9).as_secs_f64() * 1000.0,
            )
        } else {
            (0.0, 0.0)
        };
        Self {
            leaf_max_entries: cell.leaf_size,
            workload: cell.workload.label(),
            databases: cell.databases,
            workers,
            committed,
            tx_per_sec: if secs > 0.0 {
                committed as f64 / secs
            } else {
                0.0
            },
            mean_ms: results.avg().as_secs_f64() * 1000.0,
            p50_ms,
            p90_ms,
            rel_ci: results.rate_rel_ci(),
            converged,
            measured_splits: delta.restructurer.splits,
            measured_merges: delta.restructurer.merges,
            per_tx: PerTx::of(delta, committed),
        }
    }

    /// Formats a warning when measurement changed the seeded tree, which makes
    /// the cell incomparable with the other cells of its topology.
    pub(super) fn restructure_warning(&self) -> Option<String> {
        (self.measured_splits > 0 || self.measured_merges > 0).then(|| {
            format!(
                "  warning: leaf-size={} workload={} databases={} restructured the tree \
                 ({} splits, {} merges)",
                self.leaf_max_entries,
                self.workload,
                self.databases,
                self.measured_splits,
                self.measured_merges
            )
        })
    }
}

/// Counter deltas normalized by committed logical transactions.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PerTx {
    backend_ops: f64,
    obj_reads: f64,
    obj_writes: f64,
    obj_lists: f64,
    replays: f64,
    lock_calls: f64,
    direct_candidates: f64,
    direct_landed: f64,
    cross_leaf_adjacent: f64,
    cross_leaf_scattered: f64,
    cas_retries: f64,
    cas_lost_same_keys: f64,
    cas_lost_other_keys: f64,
    cas_lost_node_change: f64,
    queued_submissions: f64,
    queue_wait_ms: f64,
    scan_leaf_crossings: f64,
    leaf_writes: f64,
    leaf_write_ms: f64,
}

impl PerTx {
    fn of(delta: Stats, committed: usize) -> Self {
        let per_tx = |count: u64| count as f64 / committed.max(1) as f64;
        let per_tx_ms = |total: Duration| total.as_secs_f64() * 1e3 / committed.max(1) as f64;
        let backend = delta.backend;
        let coordinator = delta.coordinator;
        let direct = delta.direct_commit;
        Self {
            backend_ops: per_tx(backend.obj_reads + backend.obj_writes + backend.obj_lists),
            obj_reads: per_tx(backend.obj_reads),
            obj_writes: per_tx(backend.obj_writes),
            obj_lists: per_tx(backend.obj_lists),
            replays: per_tx(delta.transactions.replays),
            lock_calls: per_tx(delta.locker.calls),
            direct_candidates: per_tx(direct.candidates),
            direct_landed: per_tx(direct.landed),
            cross_leaf_adjacent: per_tx(direct.cross_leaf_adjacent),
            cross_leaf_scattered: per_tx(direct.cross_leaf_scattered),
            cas_retries: per_tx(coordinator.cas_retries),
            cas_lost_same_keys: per_tx(coordinator.cas_lost_same_keys),
            cas_lost_other_keys: per_tx(coordinator.cas_lost_other_keys),
            cas_lost_node_change: per_tx(coordinator.cas_lost_node_change),
            queued_submissions: per_tx(coordinator.queued_submissions),
            queue_wait_ms: per_tx_ms(coordinator.queue_wait),
            scan_leaf_crossings: per_tx(delta.transactions.scan_leaf_crossings),
            leaf_writes: per_tx(coordinator.leaf_writes),
            leaf_write_ms: per_tx_ms(coordinator.leaf_write_time),
        }
    }
}
