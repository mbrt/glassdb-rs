//! Fixed leaf topologies under workloads with different leaf locality.
//!
//! The scenario compares signals that could drive merges and splits. For each
//! `--leaf-sizes` value, it seeds one collection with that leaf entry limit.
//! Median splits leave each leaf with between half and all of the limit.
//! Setup then rewrites every key in its own transaction, so that leaves carry
//! their values inline. Merges stay disabled in setup and measurement, and no
//! workload adds keys, so every cell on one topology measures the same tree.
//!
//! Each cell runs one workload over `--databases` client Databases:
//!
//! - `single`: read-modify-write of one uniformly selected key;
//! - `hot`: read-modify-write of one key among the first `--hot-keys` keys;
//! - `adjacent`: read-modify-write of `--multi-keys` consecutive keys;
//! - `random`: read-modify-write of `--multi-keys` uniformly selected keys;
//! - `scan`: one read-only key scan of `--scan-keys` consecutive keys.
//!
//! Cells use the adaptive sampling of `mixed`. The report includes the lost
//! leaf CAS, cross-leaf direct-commit, and leaf write time counters, so their
//! correlation with throughput can be compared across leaf sizes.

mod result;
mod workload;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, SystemTime};

use clap::Args;
use glassdb::{Collection, Database, Error as GError, InlinePolicy, NodeSizePolicy, Stats};
use glassdb_backend::{Backend, BackendError, ListLimit};
use glassdb_bench_scale::bench::{Bench, samples_for_rel_ci};
use glassdb_bench_scale::run::{
    DriveOutcome, SplitSettlement, drive_to_significance, join_tasks_until,
    shutdown_databases_until, wait_for_split_quiet,
};
use glassdb_data::ObjectPath;
use tokio::runtime::Handle;
use tokio::time::Instant;

use super::backend;
use super::{Execution, cooldown};
pub(super) use result::RunResult;
use result::{CellResult, TopologyResult};
use workload::{KeySpace, Workload};

const COLLECTION: &str = "topology";
const SEED_BATCH: usize = 100;

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Leaf entry limits of the seeded topologies. A leaf whose values exceed
    /// the inline leaf budget splits during setup, so the limit times
    /// `--value-bytes` must fit in that budget, unless one value alone
    /// exceeds it.
    #[arg(long, value_delimiter = ',', default_value = "16,128")]
    leaf_sizes: Vec<usize>,
    /// Keys seeded in the collection.
    #[arg(long, default_value_t = 2048)]
    num_keys: usize,
    /// Bytes of every seeded and written value.
    #[arg(long, default_value_t = 64)]
    value_bytes: usize,
    /// Aggregate inline value bytes that one leaf may carry. Absent keeps the
    /// engine default.
    #[arg(long)]
    inline_leaf_bytes: Option<usize>,
    /// Workloads to measure on each topology.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "single,adjacent,random,scan"
    )]
    workloads: Vec<String>,
    /// Keys, from the first one, that a `hot` transaction selects from.
    #[arg(long, default_value_t = 64)]
    hot_keys: usize,
    /// Client Database counts to sweep. The active count is the smaller of
    /// this value and `--workers`, so every open Database is active.
    #[arg(long, value_delimiter = ',', default_value = "1,4")]
    databases: Vec<usize>,
    /// Total concurrent workers in each cell, split evenly across Databases.
    #[arg(long, default_value_t = 8)]
    workers: usize,
    /// Keys touched by one `adjacent` or `random` transaction.
    #[arg(long, default_value_t = 8)]
    multi_keys: usize,
    /// Keys listed by one `scan` transaction.
    #[arg(long, default_value_t = 64)]
    scan_keys: usize,
    /// Minimum measured window per cell.
    #[arg(long, default_value = "2s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
    /// Upper bound on a cell's measured window. A cell that stops here is
    /// flagged not-converged.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    max_duration: Duration,
    /// Target relative half-width of the throughput 95% confidence interval
    /// (`0.1` = +/-10%). `0` runs exactly `--duration`.
    #[arg(long, default_value_t = 0.1)]
    target_ci: f64,
    /// Required wall time with no completed split before measurement starts.
    #[arg(long, default_value = "10s", value_parser = glassdb_bench_scale::parse_duration)]
    split_quiet: Duration,
    /// Maximum wall time allowed for setup splits to become quiet.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    split_settle_timeout: Duration,
}

impl Options {
    /// Validates the configuration and returns the workloads in sweep order.
    fn workloads(&self) -> Result<Vec<Workload>, Box<dyn Error>> {
        if self.leaf_sizes.is_empty() || self.leaf_sizes.iter().any(|&size| size < 2) {
            return Err("--leaf-sizes must contain values >= 2".into());
        }
        if self.databases.is_empty() || self.databases.contains(&0) {
            return Err("--databases must contain values >= 1".into());
        }
        if self.workers == 0
            || self.multi_keys == 0
            || self.scan_keys == 0
            || self.hot_keys == 0
            || self.value_bytes == 0
        {
            return Err(
                "--workers, --multi-keys, --scan-keys, --hot-keys, and --value-bytes must be >= 1"
                    .into(),
            );
        }
        if self.num_keys < self.multi_keys.max(self.scan_keys).max(self.hot_keys) {
            return Err(
                "--num-keys must be at least --multi-keys, --scan-keys, and --hot-keys".into(),
            );
        }
        // Values that cannot be inline one by one never cause inline pressure,
        // so they keep every leaf size.
        let inline = self.inline_policy();
        if inline.admits_value(self.value_bytes)
            && self
                .leaf_sizes
                .iter()
                .any(|&size| size.saturating_mul(self.value_bytes) > inline.max_leaf_bytes)
        {
            return Err(
                "--leaf-sizes times --value-bytes must fit in the inline leaf budget".into(),
            );
        }
        if self.split_quiet.is_zero() {
            return Err("--split-quiet must be greater than zero".into());
        }
        if self.split_settle_timeout < self.split_quiet {
            return Err("--split-settle-timeout must be at least --split-quiet".into());
        }
        self.workloads
            .iter()
            .map(|value| Workload::parse(value.trim()).map_err(Into::into))
            .collect()
    }

    fn key_space(&self) -> KeySpace {
        KeySpace {
            num_keys: self.num_keys,
            hot_keys: self.hot_keys,
            multi_keys: self.multi_keys,
            scan_keys: self.scan_keys,
            value_bytes: self.value_bytes,
        }
    }

    fn inline_policy(&self) -> InlinePolicy {
        let default = InlinePolicy::default();
        InlinePolicy {
            max_leaf_bytes: self.inline_leaf_bytes.unwrap_or(default.max_leaf_bytes),
            ..default
        }
    }
}

/// The local policies of every Database on one topology.
#[derive(Clone, Copy)]
struct Policies {
    size: NodeSizePolicy,
    inline: InlinePolicy,
}

/// One measured combination of topology, workload, and client count.
#[derive(Clone, Copy)]
struct Cell {
    leaf_size: usize,
    workload: Workload,
    databases: usize,
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let workloads = options.workloads()?;
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        let mut topologies = Vec::new();
        let mut cells = Vec::new();
        for &leaf_size in &options.leaf_sizes {
            let name = format!("perfbenchtopology{invocation}r{run}l{leaf_size}");
            let (topology, measured) = run_topology(
                handle,
                factory.backend(),
                &name,
                leaf_size,
                &workloads,
                options,
                execution,
            )?;
            topologies.push(topology);
            cells.extend(measured);
        }
        runs.push(RunResult::new(run, topologies, cells));
    }
    Ok(runs)
}

/// Seeds one topology on `backend` and measures every workload and client
/// count on it.
fn run_topology(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    name: &str,
    leaf_size: usize,
    workloads: &[Workload],
    options: &Options,
    execution: Execution,
) -> Result<(TopologyResult, Vec<CellResult>), Box<dyn Error>> {
    let policies = Policies {
        size: fixed_policy(leaf_size)?,
        inline: options.inline_policy(),
    };
    let topology = seed_topology(handle, &backend, name, policies, options, execution)?;
    eprintln!("{}", topology.summary());
    let mut cells = Vec::new();
    for &workload in workloads {
        for &database_limit in &options.databases {
            let cell = Cell {
                leaf_size,
                workload,
                databases: database_limit.min(options.workers),
            };
            eprintln!(
                "topology: leaf-size={leaf_size} workload={} databases={}",
                workload.label(),
                cell.databases
            );
            let result = run_cell(handle, &backend, name, policies, cell, options, execution)?;
            if let Some(warning) = result.restructure_warning() {
                eprintln!("{warning}");
            }
            cells.push(result);
        }
    }
    Ok((topology, cells))
}

/// Keeps the seeded tree fixed: merges are off, and no workload adds keys to
/// exceed `leaf_max_entries`.
fn fixed_policy(leaf_max_entries: usize) -> Result<NodeSizePolicy, Box<dyn Error>> {
    Ok(NodeSizePolicy::builder()
        .leaf_max_entries(leaf_max_entries)
        .leaf_min_entries(0)
        .index_min_children(0)
        .build()?)
}

async fn open_database(
    backend: &Arc<dyn Backend>,
    name: &str,
    policies: Policies,
) -> Result<Database, GError> {
    Database::builder(name, backend.clone())
        .node_size_policy(policies.size)
        .inline_policy(policies.inline)
        .open()
        .await
}

/// Seeds the collection, publishes its values inline, and waits until the
/// resulting splits are complete.
fn seed_topology(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    name: &str,
    policies: Policies,
    options: &Options,
    execution: Execution,
) -> Result<TopologyResult, Box<dyn Error>> {
    let database = handle.block_on(open_database(backend, name, policies))?;
    let key_space = options.key_space();
    let collection = handle.block_on(async {
        let collection = database
            .root_collection()
            .create_collection_if_absent(COLLECTION)
            .await?;
        let value = key_space.value();
        let (collection, value) = (&collection, &value);
        for start in (0..options.num_keys).step_by(SEED_BATCH) {
            let end = (start + SEED_BATCH).min(options.num_keys);
            database
                .tx(|tx| async move {
                    for index in start..end {
                        tx.write(collection, &workload::key(index), value)?;
                    }
                    Ok(())
                })
                .await?;
        }
        Ok::<_, GError>(collection.clone())
    })?;
    let seeded = handle.block_on(wait_for_split_quiet(
        &database,
        options.split_quiet,
        options.split_settle_timeout,
    ))?;
    handle.block_on(workload::inline_values(
        &database,
        &collection,
        key_space,
        options.workers,
    ))?;
    let inlined = handle.block_on(wait_for_split_quiet(
        &database,
        options.split_quiet,
        options.split_settle_timeout,
    ))?;
    handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&database),
        Instant::now() + execution.drain_timeout,
    ))?;
    let nodes = handle.block_on(count_nodes(backend.as_ref(), name))?;
    Ok(TopologyResult::new(
        policies.size.leaf_max_entries(),
        options.num_keys,
        nodes,
        SplitSettlement {
            completed: seeded.completed + inlined.completed,
            elapsed: seeded.elapsed + inlined.elapsed,
        },
    ))
}

/// Counts the standalone tree nodes of every collection in the database. Only
/// the seeded collection is large enough to have any, and with at most one
/// index level these are exactly its leaves.
async fn count_nodes(backend: &dyn Backend, database_name: &str) -> Result<usize, BackendError> {
    let prefix = format!("{database_name}/");
    let limit = ListLimit::new(1000).expect("list limit is nonzero");
    let mut cursor = None;
    let mut nodes = 0;
    loop {
        let page = backend.list(&prefix, cursor.as_ref(), limit).await?;
        nodes += page
            .objects
            .iter()
            .filter(|path| {
                matches!(
                    ObjectPath::try_from(path.as_str()),
                    Ok(ObjectPath::Node { .. })
                )
            })
            .count();
        match page.next {
            Some(next) => cursor = Some(next),
            None => return Ok(nodes),
        }
    }
}

/// Opens fresh clients on the seeded topology and measures one workload.
fn run_cell(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    name: &str,
    policies: Policies,
    cell: Cell,
    options: &Options,
    execution: Execution,
) -> Result<CellResult, Box<dyn Error>> {
    let (databases, collections) = handle.block_on(open_clients(backend, name, policies, cell))?;
    // Collection binding is setup. Bracket stats only after every client opened it.
    let baselines: Vec<Stats> = databases.iter().map(Database::stats).collect();
    let bench = Arc::new(Bench::new(options.max_duration));
    let stop = Arc::new(AtomicBool::new(false));
    let target = samples_for_rel_ci(options.target_ci);

    bench.start();
    let (drive, workers, deadline) = handle.block_on(async {
        let handles = workload::spawn_workers(
            &databases,
            &collections,
            options.workers,
            cell.workload,
            options.key_space(),
            &bench,
            &stop,
        );
        let drive = drive_to_significance(
            &[&bench],
            &stop,
            target,
            options.duration,
            options.max_duration,
        )
        .await;
        let deadline = Instant::now() + execution.drain_timeout;
        let workers = join_tasks_until(handles, deadline).await;
        (drive, workers, deadline)
    });
    bench.end();
    let shutdown = handle.block_on(shutdown_databases_until(&databases, deadline));
    workers?;
    shutdown?;
    let converged = match drive {
        DriveOutcome::Converged => true,
        DriveOutcome::Capped => false,
        DriveOutcome::WorkerStopped => {
            return Err("benchmark worker stopped without reporting its error".into());
        }
    };

    let mut delta = Stats::default();
    for (database, baseline) in databases.iter().zip(baselines) {
        delta += database.stats() - baseline;
    }
    Ok(CellResult::summarize(
        cell,
        options.workers,
        bench.results(),
        converged || target == 0,
        delta,
    ))
}

async fn open_clients(
    backend: &Arc<dyn Backend>,
    name: &str,
    policies: Policies,
    cell: Cell,
) -> Result<(Vec<Database>, Vec<Collection>), GError> {
    let mut databases = Vec::with_capacity(cell.databases);
    let mut collections = Vec::with_capacity(cell.databases);
    for _ in 0..cell.databases {
        let database = open_database(backend, name, policies).await?;
        collections.push(database.open_collection(COLLECTION).await?);
        databases.push(database);
    }
    Ok((databases, collections))
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use glassdb::backend::memory::MemoryBackend;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        options: Options,
    }

    fn options(args: &[&str]) -> Options {
        let base = [
            "perfbench",
            "--num-keys=96",
            "--duration=50ms",
            "--max-duration=5s",
            "--target-ci=0",
            "--split-quiet=50ms",
            "--split-settle-timeout=10s",
        ];
        TestCli::try_parse_from(base.iter().chain(args))
            .unwrap()
            .options
    }

    #[test]
    fn invalid_cli_configurations_are_rejected() {
        let cases: &[(&[&str], &str)] = &[
            (&["--leaf-sizes=1"], "--leaf-sizes must contain values >= 2"),
            (&["--databases=0"], "--databases must contain values >= 1"),
            (
                &["--scan-keys=0"],
                "--workers, --multi-keys, --scan-keys, --hot-keys, and --value-bytes must be >= 1",
            ),
            (
                &["--value-bytes=0"],
                "--workers, --multi-keys, --scan-keys, --hot-keys, and --value-bytes must be >= 1",
            ),
            (
                &["--scan-keys=97"],
                "--num-keys must be at least --multi-keys, --scan-keys, and --hot-keys",
            ),
            (
                &["--hot-keys=97"],
                "--num-keys must be at least --multi-keys, --scan-keys, and --hot-keys",
            ),
            (
                &["--leaf-sizes=16,128", "--value-bytes=1024"],
                "--leaf-sizes times --value-bytes must fit in the inline leaf budget",
            ),
            (&["--workloads=single,warm"], "unknown workload \"warm\""),
        ];
        for (args, expected) in cases {
            let error = options(args).workloads().err().unwrap();
            assert_eq!(error.to_string(), *expected, "args: {args:?}");
        }
    }

    #[test]
    fn values_that_are_never_inline_accept_every_leaf_size() {
        let options = options(&[
            "--leaf-sizes=16,128",
            "--value-bytes=1024",
            "--inline-leaf-bytes=1023",
        ]);
        assert!(options.workloads().is_ok());
    }

    // Every workload must run on a fixed tree: the report is only comparable
    // across leaf sizes when measurement does not restructure the topology.
    #[test]
    fn every_workload_runs_on_a_fixed_seeded_topology() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let options = options(&[
            "--workers=2",
            "--databases=2",
            "--scan-keys=16",
            "--hot-keys=8",
            "--workloads=single,hot,adjacent,random,scan",
        ]);
        let workloads = options.workloads()?;
        let execution = Execution {
            runs: 1,
            run_cooldown: Duration::ZERO,
            drain_timeout: Duration::from_secs(10),
        };
        let (topology, cells) = run_topology(
            runtime.handle(),
            Arc::new(MemoryBackend::new()),
            "topologytest",
            8,
            &workloads,
            &options,
            execution,
        )?;

        // 96 keys in leaves of 4..=8 entries, below one root index.
        let topology = serde_json::to_value(&topology)?;
        let nodes = topology["nodes"].as_u64().unwrap();
        assert!((12..=24).contains(&nodes), "nodes: {nodes}");
        let cells = serde_json::to_value(&cells)?;
        let summary: Vec<_> = cells
            .as_array()
            .unwrap()
            .iter()
            .map(|cell| {
                (
                    cell["workload"].as_str().unwrap().to_string(),
                    cell["committed"].as_u64().unwrap() > 0,
                    cell["measuredSplits"].as_u64().unwrap(),
                    cell["measuredMerges"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            summary,
            ["single", "hot", "adjacent", "random", "scan"].map(|workload| (
                workload.to_string(),
                true,
                0,
                0
            ))
        );
        Ok(())
    }
}
