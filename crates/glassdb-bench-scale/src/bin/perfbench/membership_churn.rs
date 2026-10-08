//! Measures creates and deletes that change the key membership of one leaf
//! while other transactions scan it.
//!
//! Direct churners create and delete keys with single-key transactions, so
//! they commit direct (ADR-061). Locked churners do the same after a scan in
//! the same transaction, which keeps them from a direct commit, so they take
//! key locks and the membership lock of the leaf. Scanners scan the whole
//! collection. Each role runs in its own database instance, so each instance
//! reads the transaction records of the others from the backend.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use clap::Args;
use futures::future::join_all;
use glassdb::{Collection, Database, Error as GError, KeyScan, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::{Bench, Results};
use glassdb_bench_scale::run::{join_tasks_until, shutdown_databases_until};
use serde::Serialize;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use super::backend;
use super::{Execution, cooldown};

const COLLECTION: &[u8] = b"churn";
const VALUE_BYTES: usize = 128;
/// The keys that each churn worker creates and deletes in turn. Few keys keep
/// the collection in one leaf.
const KEYS_PER_WORKER: usize = 4;

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Workers of the direct churner instance. Zero leaves it idle.
    #[arg(long, default_value_t = 4)]
    direct_workers: usize,
    /// Workers of the locked churner instance. Zero leaves it idle.
    #[arg(long, default_value_t = 4)]
    locked_workers: usize,
    /// Workers of the scanner instance. Zero leaves it idle.
    #[arg(long, default_value_t = 8)]
    scanner_workers: usize,
    /// Wall time of the load before the measurement.
    #[arg(long, default_value = "5s", value_parser = glassdb_bench_scale::parse_duration)]
    warmup: Duration,
    /// Measured wall time of each run. The results vary much from run to
    /// run, so compare at least 4 runs of 60 s.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
}

/// The load of one database instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    DirectChurner,
    LockedChurner,
    Scanner,
}

const ROLES: [Role; 3] = [Role::DirectChurner, Role::LockedChurner, Role::Scanner];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunResult {
    run: usize,
    /// One entry for each database instance.
    instances: Vec<InstanceResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InstanceResult {
    role: &'static str,
    workers: usize,
    committed: usize,
    tx_per_sec: f64,
    p50_ms: f64,
    p90_ms: f64,
    replays: u64,
    lock_calls: u64,
    direct_landed: u64,
    obj_reads: u64,
    obj_writes: u64,
    coordinator_rounds: u64,
    coordinator_cas_retries: u64,
    splits: u64,
    merges: u64,
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        eprintln!("membership-churn: run={run}");
        let name = format!("perfbenchchurn{invocation}r{run}");
        let instances = run_once(handle, factory.backend(), &name, options, execution)?;
        runs.push(RunResult { run, instances });
    }
    Ok(runs)
}

fn run_once(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    name: &str,
    options: &Options,
    execution: Execution,
) -> Result<Vec<InstanceResult>, Box<dyn Error>> {
    handle.block_on(seed(&backend, name, execution))?;
    let databases: Vec<Database> = handle.block_on(async {
        let opens = ROLES
            .iter()
            .map(|_| Database::builder(name, backend.clone()).open());
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
    let benches: Vec<Arc<Bench>> = ROLES
        .iter()
        .map(|_| Arc::new(Bench::new(options.duration)))
        .collect();
    // The warmup samples are not reported. The warmup uses a bench only so
    // that it retries in-doubt outcomes as the measurement does.
    let warmup_bench = Arc::new(Bench::new(options.warmup));
    let mut workers: Vec<JoinHandle<Result<(), GError>>> = Vec::new();
    for (index, &role) in ROLES.iter().enumerate() {
        for worker in 0..options.workers(role) {
            let load = Load {
                db: databases[index].clone(),
                collection: collections[index].clone(),
                role,
                worker,
                stop: stop.clone(),
                recording: recording.clone(),
                bench: benches[index].clone(),
                warmup_bench: warmup_bench.clone(),
            };
            workers.push(handle.spawn(load.run()));
        }
    }

    let (deltas, results) = handle.block_on(async {
        tokio::time::sleep(options.warmup).await;
        let start: Vec<Stats> = databases.iter().map(Database::stats).collect();
        for bench in &benches {
            bench.start();
        }
        recording.store(true, Ordering::Relaxed);
        tokio::time::sleep(options.duration).await;
        recording.store(false, Ordering::Relaxed);
        for bench in &benches {
            bench.end();
        }
        // Transactions that are still running can add samples after the end.
        let results: Vec<Results> = benches.iter().map(|bench| bench.results()).collect();
        let deltas: Vec<Stats> = databases
            .iter()
            .zip(&start)
            .map(|(db, start)| db.stats() - *start)
            .collect();
        (deltas, results)
    });
    stop.store(true, Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + execution.drain_timeout;
    let joined = handle.block_on(join_tasks_until(workers, deadline));
    let shutdown = handle.block_on(shutdown_databases_until(&databases, deadline));
    joined?;
    shutdown?;

    Ok(ROLES
        .iter()
        .zip(&results)
        .zip(&deltas)
        .map(|((&role, results), stats)| {
            instance_result(role, options.workers(role), results, stats)
        })
        .collect())
}

async fn seed(
    backend: &Arc<dyn Backend>,
    name: &str,
    execution: Execution,
) -> Result<(), Box<dyn Error>> {
    let db = Database::builder(name, backend.clone()).open().await?;
    db.root_collection()
        .create_collection_if_absent(COLLECTION)
        .await?;
    shutdown_databases_until(
        std::slice::from_ref(&db),
        tokio::time::Instant::now() + execution.drain_timeout,
    )
    .await?;
    Ok(())
}

impl Options {
    fn workers(&self, role: Role) -> usize {
        match role {
            Role::DirectChurner => self.direct_workers,
            Role::LockedChurner => self.locked_workers,
            Role::Scanner => self.scanner_workers,
        }
    }
}

/// The transactions of one worker of one database instance.
struct Load {
    db: Database,
    collection: Collection,
    role: Role,
    worker: usize,
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

    /// Creates a key in even rounds and deletes it in the next round, so that
    /// each churn transaction changes the key membership.
    async fn once(&self, round: usize) -> Result<(), GError> {
        let c = &self.collection;
        let key = format!(
            "{}-{}-{}",
            self.role.label(),
            self.worker,
            (round / 2) % KEYS_PER_WORKER
        )
        .into_bytes();
        let create = round.is_multiple_of(2);
        match self.role {
            Role::Scanner => c.scan_keys(KeyScan::all()).await.map(|_| ()),
            Role::DirectChurner if create => c.write(&key, &[0x5a; VALUE_BYTES]).await,
            Role::DirectChurner => c.delete(&key).await,
            Role::LockedChurner => {
                let key = &key;
                self.db
                    .tx(|tx| async move {
                        tx.scan_keys(c, KeyScan::all()).await?;
                        if create {
                            tx.write(c, key, &[0x5a; VALUE_BYTES])
                        } else {
                            tx.delete(c, key)
                        }
                    })
                    .await
            }
        }
    }
}

fn instance_result(role: Role, workers: usize, results: &Results, stats: &Stats) -> InstanceResult {
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
        workers,
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
        obj_reads: stats.backend.obj_reads,
        obj_writes: stats.backend.obj_writes,
        coordinator_rounds: stats.coordinator.rounds,
        coordinator_cas_retries: stats.coordinator.cas_retries,
        splits: stats.restructurer.splits,
        merges: stats.restructurer.merges,
    }
}

impl Role {
    fn label(self) -> &'static str {
        match self {
            Role::DirectChurner => "direct-churner",
            Role::LockedChurner => "locked-churner",
            Role::Scanner => "scanner",
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    // The scenario measures the two commit paths of a membership change, so
    // each churner must use its own path. A direct churner can
    // also fall back to locks, while a locked churner holds the membership
    // lock, and a scanner can take locks after a failed validation.
    #[test]
    fn each_churner_uses_its_own_commit_path() -> Result<(), Box<dyn Error>> {
        let cli = crate::Cli::try_parse_from([
            "perfbench",
            "membership-churn",
            "--direct-workers=1",
            "--locked-workers=1",
            "--scanner-workers=1",
            // Without a warmup, the first transaction of each worker starts
            // in the measurement.
            "--warmup=0s",
            "--duration=3s",
        ])?;
        let crate::Command::MembershipChurn(options) = &cli.command else {
            unreachable!("the command is membership-churn");
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let factory = runtime.block_on(cli.backend.initialize())?;

        let runs = run(runtime.handle(), &factory, options, crate::execution(&cli))?;

        let [direct, locked, scanner] = &runs[0].instances[..] else {
            unreachable!("one instance for each role");
        };
        assert!(direct.committed > 0 && direct.direct_landed > 0);
        // A locked churn transaction can take some seconds, so this short run
        // checks that it locks, not that it commits.
        assert!(locked.direct_landed == 0 && locked.lock_calls > 0);
        assert!(scanner.committed > 0);
        Ok(())
    }
}
