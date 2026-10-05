//! Measures a stable load under which the topology policies of database
//! instances disagree: some instances split a leaf, and another merges it back.
//!
//! The collection has two keys in one leaf. Splitter instances write the left
//! key or the right key. The instances of one key lose leaf CASes to the
//! instances of the other key, and a split between the keys removes this
//! avoidable time (ADR-074). Each half then has one key, so it cannot split again. A merger
//! instance scans both keys. After the split, each scan crosses into the right
//! leaf, and a merge removes this avoidable time. Each instance decides only on its own
//! transactions, so the load does not change but the leaf splits and merges
//! again and again.
//!
//! The fixed policies keep the topology that the setup made, to show the
//! throughput of each topology without structural changes.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use futures::future::join_all;
use glassdb::{
    Collection, Database, Error as GError, KeyScan, NodeSizePolicy, Stats, TopologyPolicy,
};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::{Bench, Results};
use glassdb_bench_scale::run::{join_tasks_until, shutdown_databases_until};
use serde::Serialize;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::backend;
use super::{Execution, cooldown};

const COLLECTION: &[u8] = b"fight";
const LEFT_KEY: &[u8] = b"left";
const RIGHT_KEY: &[u8] = b"right";
const VALUE_BYTES: usize = 128;
/// The time between two samples of the structural changes in the timeline.
const TIMELINE_INTERVAL: Duration = Duration::from_secs(1);
/// The maximum wall time of the setup split of `fixed-2`.
const SETUP_SPLIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Topology policies to compare: `avoidable` (the default policy),
    /// `fixed-1` (one leaf, no changes), and `fixed-2` (one leaf for each key,
    /// no changes).
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "avoidable,fixed-1,fixed-2"
    )]
    policies: Vec<String>,
    /// Splitter instances for each key.
    #[arg(long, default_value_t = 1)]
    splitter_instances: usize,
    /// Workers of each splitter instance.
    #[arg(long, default_value_t = 4)]
    splitter_workers: usize,
    /// Workers of the merger instance. With fewer workers, the scans cross
    /// too seldom to merge the leaves again after each split.
    #[arg(long, default_value_t = 8)]
    merger_workers: usize,
    /// Wall time of the load before the measurement.
    #[arg(long, default_value = "10s", value_parser = glassdb_bench_scale::parse_duration)]
    warmup: Duration,
    /// Measured wall time of each cell. One split and merge takes about 5 to
    /// 20 s with the S3 delays, so shorter windows have few of them.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy {
    Avoidable,
    FixedOne,
    FixedTwo,
}

/// The load of one database instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    LeftSplitter,
    RightSplitter,
    Merger,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunResult {
    run: usize,
    cells: Vec<CellResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CellResult {
    policy: &'static str,
    /// The splits and merges of all instances in the warmup.
    warmup_splits: u64,
    warmup_merges: u64,
    /// The splits and merges of all instances in the measurement.
    splits: u64,
    merges: u64,
    /// One entry for each database instance.
    instances: Vec<InstanceResult>,
    /// The splits and merges of each instance in each wall second: first the
    /// seconds of the warmup, then of the measurement.
    timeline: Vec<TimelineEntry>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstanceResult {
    role: &'static str,
    committed: usize,
    tx_per_sec: f64,
    p50_ms: f64,
    p90_ms: f64,
    replays: u64,
    lock_calls: u64,
    direct_landed: u64,
    splits: u64,
    merges: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TimelineEntry {
    second: u64,
    /// The splits and merges of each instance, in the order of `instances`.
    splits: Vec<u64>,
    merges: Vec<u64>,
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let policies = options
        .policies
        .iter()
        .map(|value| Policy::parse(value).ok_or_else(|| format!("unknown policy {value:?}")))
        .collect::<Result<Vec<_>, _>>()?;
    if options.splitter_instances == 0
        || options.splitter_workers == 0
        || options.merger_workers == 0
    {
        return Err(
            "--splitter-instances, --splitter-workers, and --merger-workers must be positive"
                .into(),
        );
    }
    let roles = roles(options.splitter_instances);
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        let mut cells = Vec::new();
        for (index, &policy) in policies.iter().enumerate() {
            eprintln!("split-merge-fight: run={run} policy={}", policy.label());
            // Each cell needs its own database, also when a policy repeats.
            let name = format!(
                "perfbenchfight{invocation}r{run}c{index}{}",
                policy.label().replace('-', "")
            );
            let cell = run_cell(
                handle,
                factory.backend(),
                &name,
                &roles,
                policy,
                options,
                execution,
            )?;
            eprintln!(
                "split-merge-fight: splits={} merges={} (warmup {}/{})",
                cell.splits, cell.merges, cell.warmup_splits, cell.warmup_merges
            );
            cells.push(cell);
        }
        runs.push(RunResult { run, cells });
    }
    Ok(runs)
}

/// Returns the role of each database instance: the splitter instances of the
/// left key, then of the right key, then the merger instance.
fn roles(splitter_instances: usize) -> Vec<Role> {
    [Role::LeftSplitter, Role::RightSplitter]
        .into_iter()
        .flat_map(|role| std::iter::repeat_n(role, splitter_instances))
        .chain([Role::Merger])
        .collect()
}

fn run_cell(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    name: &str,
    roles: &[Role],
    policy: Policy,
    options: &Options,
    execution: Execution,
) -> Result<CellResult, Box<dyn Error>> {
    handle.block_on(seed(&backend, name, policy, execution))?;
    let databases: Vec<Database> = handle.block_on(async {
        let opens = roles.iter().map(|_| open(&backend, name, policy));
        join_all(opens).await.into_iter().collect::<Result<_, _>>()
    })?;
    let collections: Vec<Collection> = handle.block_on(async {
        let opens = databases
            .iter()
            .map(|db| async move { db.root_collection().open_collection(COLLECTION).await });
        join_all(opens).await.into_iter().collect::<Result<_, _>>()
    })?;

    let stop = Arc::new(AtomicBool::new(false));
    let recording = Arc::new(AtomicBool::new(false));
    let benches: Vec<Arc<Bench>> = roles
        .iter()
        .map(|_| Arc::new(Bench::new(options.duration)))
        .collect();
    // The warmup samples are not reported. The warmup uses a bench only so
    // that it retries in-doubt outcomes as the measurement does.
    let warmup_bench = Arc::new(Bench::new(options.warmup));
    let mut workers: Vec<JoinHandle<Result<(), GError>>> = Vec::new();
    for (index, &role) in roles.iter().enumerate() {
        let count = match role {
            Role::LeftSplitter | Role::RightSplitter => options.splitter_workers,
            Role::Merger => options.merger_workers,
        };
        for _ in 0..count {
            let load = Load {
                collection: collections[index].clone(),
                role,
                stop: stop.clone(),
                recording: recording.clone(),
                bench: benches[index].clone(),
                warmup_bench: warmup_bench.clone(),
            };
            workers.push(handle.spawn(load.run()));
        }
    }

    let (warmup, deltas, results, timeline) = handle.block_on(async {
        let mut timeline = Timeline::new(&databases);
        let warmup_start = snapshot(&databases);
        timeline.sample_for(&databases, options.warmup).await;
        let measured_start = snapshot(&databases);
        for bench in &benches {
            bench.start();
        }
        recording.store(true, Ordering::Relaxed);
        timeline.sample_for(&databases, options.duration).await;
        recording.store(false, Ordering::Relaxed);
        for bench in &benches {
            bench.end();
        }
        // Transactions that are still running can add samples after the end.
        let results: Vec<Results> = benches.iter().map(|bench| bench.results()).collect();
        let measured_end = snapshot(&databases);
        let warmup = Changes::sum(&deltas(&measured_start, &warmup_start));
        (
            warmup,
            deltas(&measured_end, &measured_start),
            results,
            timeline.entries,
        )
    });
    let measured = Changes::sum(&deltas);
    stop.store(true, Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + execution.drain_timeout;
    let joined = handle.block_on(join_tasks_until(workers, deadline));
    let shutdown = handle.block_on(shutdown_databases_until(&databases, deadline));
    joined?;
    shutdown?;

    let instances = roles
        .iter()
        .zip(&results)
        .zip(&deltas)
        .map(|((role, results), stats)| instance_result(*role, results, stats))
        .collect();
    Ok(CellResult {
        policy: policy.label(),
        warmup_splits: warmup.splits,
        warmup_merges: warmup.merges,
        splits: measured.splits,
        merges: measured.merges,
        instances,
        timeline,
    })
}

/// Creates the collection with both keys in one leaf. With `fixed-2`, the
/// setup instance then splits the leaf between the keys.
async fn seed(
    backend: &Arc<dyn Backend>,
    name: &str,
    policy: Policy,
    execution: Execution,
) -> Result<(), Box<dyn Error>> {
    let mut builder = Database::builder(name, backend.clone());
    if policy == Policy::FixedTwo {
        builder = builder
            .topology_policy(TopologyPolicy::SizeCauses)
            .node_size_policy(fixed_leaves(1)?);
    }
    let db = builder.open().await?;
    let collection = db
        .root_collection()
        .create_collection_if_absent(COLLECTION)
        .await?;
    let c = &collection;
    db.tx(|tx| async move {
        tx.write(c, LEFT_KEY, &value(0))?;
        tx.write(c, RIGHT_KEY, &value(0))?;
        Ok(())
    })
    .await?;
    // The leaf of the collection is the only leaf that can split: the leaf
    // of the root collection has one entry.
    if policy == Policy::FixedTwo {
        wait_for_one_split(&db).await?;
    }
    shutdown_databases_until(
        std::slice::from_ref(&db),
        tokio::time::Instant::now() + execution.drain_timeout,
    )
    .await?;
    Ok(())
}

async fn wait_for_one_split(db: &Database) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + SETUP_SPLIT_TIMEOUT;
    while db.stats().restructurer.splits == 0 {
        if Instant::now() >= deadline {
            return Err("the setup did not split the leaf".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

async fn open(
    backend: &Arc<dyn Backend>,
    name: &str,
    policy: Policy,
) -> Result<Database, Box<dyn Error>> {
    let builder = Database::builder(name, backend.clone());
    let builder = match policy {
        Policy::Avoidable => builder.topology_policy(TopologyPolicy::AvoidableTime),
        // The size causes do not change a leaf below the soft caps, above the
        // underfull threshold, and with no inline pressure. The values of
        // this load are small, so their leaves have no inline pressure.
        Policy::FixedOne | Policy::FixedTwo => {
            let sizes = fixed_leaves(NodeSizePolicy::default().leaf_max_entries())?;
            builder
                .topology_policy(TopologyPolicy::SizeCauses)
                .node_size_policy(sizes)
        }
    };
    Ok(builder.open().await?)
}

/// Returns a node size policy with leaves of at most `entries` entries and no
/// underfull threshold.
fn fixed_leaves(entries: usize) -> Result<NodeSizePolicy, Box<dyn Error>> {
    Ok(NodeSizePolicy::builder()
        .leaf_max_entries(entries)
        .leaf_min_entries(0)
        .index_min_children(0)
        .build()?)
}

/// The transactions of one worker of one database instance.
struct Load {
    collection: Collection,
    role: Role,
    stop: Arc<AtomicBool>,
    /// Whether the measurement is running.
    recording: Arc<AtomicBool>,
    bench: Arc<Bench>,
    warmup_bench: Arc<Bench>,
}

impl Load {
    async fn run(self) -> Result<(), GError> {
        let mut round = 0usize;
        while !self.stop.load(Ordering::Relaxed) {
            let bench = if self.recording.load(Ordering::Relaxed) {
                &self.bench
            } else {
                &self.warmup_bench
            };
            bench.measure(|| self.once(round)).await?;
            round += 1;
        }
        Ok(())
    }

    async fn once(&self, round: usize) -> Result<(), GError> {
        let c = &self.collection;
        match self.role {
            Role::LeftSplitter => c.write(LEFT_KEY, &value(round)).await,
            Role::RightSplitter => c.write(RIGHT_KEY, &value(round)).await,
            Role::Merger => c.scan_keys(KeyScan::all()).await.map(|_| ()),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Changes {
    splits: u64,
    merges: u64,
}

impl Changes {
    fn of(stats: &Stats) -> Self {
        Changes {
            splits: stats.restructurer.splits,
            merges: stats.restructurer.merges,
        }
    }

    /// Returns the changes of all instances in `deltas`.
    fn sum(deltas: &[Stats]) -> Self {
        deltas
            .iter()
            .map(Changes::of)
            .fold(Changes::default(), |sum, changes| Changes {
                splits: sum.splits + changes.splits,
                merges: sum.merges + changes.merges,
            })
    }
}

fn snapshot(databases: &[Database]) -> Vec<Stats> {
    databases.iter().map(Database::stats).collect()
}

/// Returns the stats of each instance between the snapshots `start` and
/// `end`.
fn deltas(end: &[Stats], start: &[Stats]) -> Vec<Stats> {
    end.iter()
        .zip(start)
        .map(|(end, start)| *end - *start)
        .collect()
}

/// The structural changes of each instance in each second.
struct Timeline {
    last: Vec<Stats>,
    entries: Vec<TimelineEntry>,
}

impl Timeline {
    fn new(databases: &[Database]) -> Self {
        Self {
            last: snapshot(databases),
            entries: Vec::new(),
        }
    }

    async fn sample_for(&mut self, databases: &[Database], duration: Duration) {
        let end = Instant::now() + duration;
        while Instant::now() < end {
            tokio::time::sleep(TIMELINE_INTERVAL.min(end - Instant::now())).await;
            let now = snapshot(databases);
            let changes: Vec<Changes> = deltas(&now, &self.last).iter().map(Changes::of).collect();
            self.last = now;
            self.entries.push(TimelineEntry {
                second: self.entries.len() as u64 + 1,
                splits: changes.iter().map(|change| change.splits).collect(),
                merges: changes.iter().map(|change| change.merges).collect(),
            });
        }
    }
}

fn instance_result(role: Role, results: &Results, stats: &Stats) -> InstanceResult {
    let seconds = results.tot_duration.as_secs_f64();
    let committed = results.samples.len();
    let percentile = |p| {
        if committed == 0 {
            0.0
        } else {
            results.percentile(p).as_secs_f64() * 1000.0
        }
    };
    InstanceResult {
        role: role.label(),
        committed,
        tx_per_sec: if seconds > 0.0 {
            committed as f64 / seconds
        } else {
            0.0
        },
        p50_ms: percentile(0.5),
        p90_ms: percentile(0.9),
        replays: stats.transactions.replays,
        lock_calls: stats.locker.calls,
        direct_landed: stats.direct_commit.landed,
        splits: stats.restructurer.splits,
        merges: stats.restructurer.merges,
    }
}

impl Policy {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "avoidable" => Some(Policy::Avoidable),
            "fixed-1" => Some(Policy::FixedOne),
            "fixed-2" => Some(Policy::FixedTwo),
            _ => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Policy::Avoidable => "avoidable",
            Policy::FixedOne => "fixed-1",
            Policy::FixedTwo => "fixed-2",
        }
    }
}

impl Role {
    fn label(self) -> &'static str {
        match self {
            Role::LeftSplitter => "left-splitter",
            Role::RightSplitter => "right-splitter",
            Role::Merger => "merger",
        }
    }
}

fn value(round: usize) -> Vec<u8> {
    let mut value = vec![0x5a; VALUE_BYTES];
    value[..8].copy_from_slice(&(round as u64).to_le_bytes());
    value
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    // The fixed policies are the baselines of the fight, so they must keep
    // the topology of the setup.
    #[test]
    fn fixed_policies_make_no_structural_change() -> Result<(), Box<dyn Error>> {
        let cli = crate::Cli::try_parse_from([
            "perfbench",
            "split-merge-fight",
            "--policies=fixed-1,fixed-2",
            "--splitter-workers=1",
            "--merger-workers=1",
            "--warmup=1s",
            "--duration=1s",
        ])?;
        let crate::Command::SplitMergeFight(options) = &cli.command else {
            unreachable!("the command is split-merge-fight");
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let factory = runtime.block_on(cli.backend.initialize())?;

        let runs = run(runtime.handle(), &factory, options, crate::execution(&cli))?;

        let changes: Vec<_> = runs[0]
            .cells
            .iter()
            .map(|cell| {
                (
                    cell.policy,
                    cell.warmup_splits + cell.splits,
                    cell.warmup_merges + cell.merges,
                )
            })
            .collect();
        assert_eq!(changes, vec![("fixed-1", 0, 0), ("fixed-2", 0, 0)]);
        Ok(())
    }
}
