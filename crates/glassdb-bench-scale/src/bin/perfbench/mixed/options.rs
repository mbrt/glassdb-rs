use std::error::Error;
use std::time::Duration;

use clap::Args;

use super::workload::{KeySelection, SHAPE_COUNT};

#[derive(Clone, Args)]
pub(crate) struct Options {
    /// Minimum measured interval per cell. The cell keeps running all shapes
    /// concurrently past this until every shape reaches `--window-commits`,
    /// or `--max-duration` is hit.
    #[arg(long, default_value = "2s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) duration: Duration,
    /// Upper bound on a cell's measured interval: the cell stops here even if
    /// a shape (typically a heavily-contended write shape) has not yet reached
    /// `--window-commits`; such a shape's result is flagged not-converged.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) max_duration: Duration,
    /// Mean commits per throughput window that every shape must reach. The
    /// cell runs until each shape commits this many times the window count, or
    /// until `--max-duration`. The counting noise of one window has a standard
    /// deviation of about `1/sqrt(n)`, so about 14% with the default. `0`
    /// disables adaptivity: run exactly `--duration`.
    #[arg(long, default_value_t = 50)]
    pub(super) window_commits: u64,
    /// Total concurrent workers per shape, split as evenly as possible across
    /// the Databases. A comma-separated list sweeps several worker counts.
    #[arg(long, value_delimiter = ',', default_value = "8")]
    workers_per_shape: Vec<usize>,
    /// Maximum client `Database`s in each cell. The active count is the smaller
    /// of this limit and the worker count, so every open Database runs every
    /// shape and has a distinct home collection. A comma-separated list sweeps
    /// several limits.
    #[arg(long, value_delimiter = ',', default_value = "4")]
    databases: Vec<usize>,
    /// Maximum collections in each cell. Database `i` has collection
    /// `i % collections` as its home, so fewer collections than Databases
    /// make Databases share a home collection. Absent: one for each Database.
    #[arg(long)]
    collections: Option<usize>,
    /// Keys touched by the multi-key shapes (`rwMany`, `roMulti`); clamped to
    /// the pool size in the `hi` mode.
    #[arg(long, default_value_t = 10)]
    pub(super) multi_keys: usize,
    /// Key-pool size per collection for the `lo` (spread) mode. Each worker
    /// draws from its own stripe of the pool, so the pool must give every
    /// stripe at least `--multi-keys` keys.
    #[arg(long, default_value_t = 5000)]
    pub(super) num_keys: usize,
    /// Percentage of `lo`-mode transactions that draw keys from the whole
    /// pool instead of from their worker's stripe. Only these transactions
    /// can conflict with the transactions of another worker.
    #[arg(long, default_value_t = 1)]
    pub(super) shared_pct: u8,
    /// Key-pool size for the `hi` (hot) mode.
    #[arg(long, default_value_t = 8)]
    pub(super) hot_keys: usize,
    /// Contention modes to sweep.
    #[arg(long, value_delimiter = ',', default_value = "lo,hi")]
    modes: Vec<String>,
    /// Home-collection affinity percentages to sweep. At 0%, a Database picks
    /// uniformly from every collection. At 100%, it always uses its own.
    #[arg(long, value_delimiter = ',', default_value = "0,25,50,75,100")]
    affinities: Vec<u8>,
    /// Wall time without topology changes that makes a state settled: without
    /// splits, merges, and garbage collection progress after the seed, and
    /// without splits and merges at the end of a cell's warm-up. Model-time
    /// acceleration also compresses the background cadence of these changes.
    #[arg(long, default_value = "10s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) quiet_period: Duration,
    /// Maximum wall time for the seed to settle. The run fails after it.
    #[arg(long, default_value = "180s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) seed_timeout: Duration,
    /// Maximum time for the full GCs after the seed and after each warm-up.
    /// The records of the last safety horizon of a load wait for it, so this
    /// must be longer than the pending timeout plus the clock skew allowance
    /// of the database. The seed fails after it; a cell measures with the
    /// garbage that is left.
    #[arg(long, default_value = "600s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) gc_timeout: Duration,
    /// Maximum wall time of a cell's warm-up. The cell measures after it, also
    /// when topology changes continue, and its result says so.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) warmup_timeout: Duration,
    /// Minimum model time of each cell's warm-up, before its quiet period can
    /// end it. The topology policy decides on windows of 1 s of model time,
    /// so a shorter warm-up can end before a load causes its first split.
    #[arg(long, default_value = "5s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) warmup_minimum: Duration,
}

impl Options {
    /// Validates the mixed-workload configuration and enumerates its cells.
    pub(super) fn cell_dimensions(&self) -> Result<Vec<CellDimension>, Box<dyn Error>> {
        self.validate()?;
        let modes = parse_modes(&self.modes)?;
        if modes.contains(&Mode::Lo) {
            self.validate_stripes()?;
        }
        let mut dimensions = Vec::new();
        for mode in modes {
            for &affinity_pct in &self.affinities {
                for &database_limit in &self.databases {
                    for &workers_per_shape in &self.workers_per_shape {
                        let databases = database_limit.min(workers_per_shape);
                        dimensions.push(CellDimension {
                            mode,
                            affinity_pct,
                            database_limit,
                            databases,
                            collections: self
                                .collections
                                .map_or(databases, |limit| limit.min(databases)),
                            workers_per_shape,
                        });
                    }
                }
            }
        }
        Ok(dimensions)
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.workers_per_shape.is_empty() || self.workers_per_shape.contains(&0) {
            return Err("--workers-per-shape must contain values >= 1".into());
        }
        if self.databases.is_empty() || self.databases.contains(&0) {
            return Err("--databases must contain values >= 1".into());
        }
        if self.collections == Some(0) {
            return Err("--collections must be >= 1".into());
        }
        if self.affinities.is_empty() || self.affinities.iter().any(|&a| a > 100) {
            return Err("--affinities must contain percentages from 0 through 100".into());
        }
        if self.shared_pct > 100 {
            return Err("--shared-pct must be a percentage from 0 through 100".into());
        }
        if self.quiet_period.is_zero() {
            return Err("--quiet-period must be greater than zero".into());
        }
        if self.seed_timeout < self.quiet_period {
            return Err("--seed-timeout must be at least --quiet-period".into());
        }
        Ok(())
    }

    /// Rejects a `lo` cell whose stripes are smaller than a multi-key
    /// transaction, which would silently shrink its transactions.
    fn validate_stripes(&self) -> Result<(), Box<dyn Error>> {
        let max_workers = self.workers_per_shape.iter().copied().max().unwrap_or(1);
        let workers = SHAPE_COUNT * max_workers;
        let stripe = self.num_keys / workers;
        if stripe < self.multi_keys {
            return Err(format!(
                "--num-keys={} gives {workers} workers stripes of {stripe} keys, \
                 fewer than --multi-keys={}",
                self.num_keys, self.multi_keys
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct CellDimension {
    pub(super) mode: Mode,
    pub(super) affinity_pct: u8,
    pub(super) database_limit: usize,
    pub(super) databases: usize,
    pub(super) collections: usize,
    pub(super) workers_per_shape: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    Lo,
    Hi,
}

impl Mode {
    pub(super) fn label(self) -> &'static str {
        match self {
            Mode::Lo => "lo",
            Mode::Hi => "hi",
        }
    }

    /// Returns the key-pool size for this contention mode.
    pub(super) fn pool_size(self, options: &Options) -> usize {
        match self {
            Mode::Lo => options.num_keys.max(1),
            Mode::Hi => options.hot_keys.max(1),
        }
    }

    /// Returns how workers choose keys in this contention mode.
    pub(super) fn key_selection(self, options: &Options) -> KeySelection {
        match self {
            Mode::Lo => KeySelection::Stripes {
                shared_pct: options.shared_pct,
            },
            Mode::Hi => KeySelection::Shared,
        }
    }
}

fn parse_modes(values: &[String]) -> Result<Vec<Mode>, Box<dyn Error>> {
    values
        .iter()
        .map(|value| match value.trim() {
            "lo" => Ok(Mode::Lo),
            "hi" => Ok(Mode::Hi),
            other => Err(format!("unknown mode {other:?} (expected lo|hi)").into()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        options: Options,
    }

    fn dimension_snapshot(args: &[&str]) -> Result<String, Box<dyn Error>> {
        let cli = TestCli::try_parse_from(args)?;
        Ok(cli
            .options
            .cell_dimensions()?
            .into_iter()
            .map(|cell| {
                format!(
                    "{}:{}:limit{}:db{}:c{}:w{}",
                    cell.mode.label(),
                    cell.affinity_pct,
                    cell.database_limit,
                    cell.databases,
                    cell.collections,
                    cell.workers_per_shape
                )
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }

    #[test]
    fn cli_arguments_enumerate_stable_cell_snapshots() -> Result<(), Box<dyn Error>> {
        let cases: &[(&[&str], &str)] = &[
            (
                &["perfbench"],
                "lo:0:limit4:db4:c4:w8\n\
                 lo:25:limit4:db4:c4:w8\n\
                 lo:50:limit4:db4:c4:w8\n\
                 lo:75:limit4:db4:c4:w8\n\
                 lo:100:limit4:db4:c4:w8\n\
                 hi:0:limit4:db4:c4:w8\n\
                 hi:25:limit4:db4:c4:w8\n\
                 hi:50:limit4:db4:c4:w8\n\
                 hi:75:limit4:db4:c4:w8\n\
                 hi:100:limit4:db4:c4:w8",
            ),
            (
                &[
                    "perfbench",
                    "--modes",
                    "hi,lo",
                    "--affinities",
                    "100",
                    "--databases",
                    "1,3",
                    "--workers-per-shape",
                    "1,5",
                ],
                "hi:100:limit1:db1:c1:w1\n\
                 hi:100:limit1:db1:c1:w5\n\
                 hi:100:limit3:db1:c1:w1\n\
                 hi:100:limit3:db3:c3:w5\n\
                 lo:100:limit1:db1:c1:w1\n\
                 lo:100:limit1:db1:c1:w5\n\
                 lo:100:limit3:db1:c1:w1\n\
                 lo:100:limit3:db3:c3:w5",
            ),
            (
                &[
                    "perfbench",
                    "--modes",
                    "hi",
                    "--affinities",
                    "100",
                    "--workers-per-shape",
                    "126",
                ],
                "hi:100:limit4:db4:c4:w126",
            ),
            (
                &[
                    "perfbench",
                    "--modes",
                    "lo",
                    "--affinities",
                    "0",
                    "--databases",
                    "1,5",
                    "--collections",
                    "2",
                ],
                "lo:0:limit1:db1:c1:w8\n\
                 lo:0:limit5:db5:c2:w8",
            ),
        ];

        for (args, expected) in cases {
            assert_eq!(dimension_snapshot(args)?, *expected, "args: {args:?}");
        }
        Ok(())
    }

    #[test]
    fn invalid_cli_configurations_are_rejected() -> Result<(), Box<dyn Error>> {
        let cases: &[(&[&str], &str)] = &[
            (
                &["perfbench", "--workers-per-shape", "0"],
                "--workers-per-shape must contain values >= 1",
            ),
            (
                &["perfbench", "--databases", "0"],
                "--databases must contain values >= 1",
            ),
            (
                &["perfbench", "--collections", "0"],
                "--collections must be >= 1",
            ),
            (
                &["perfbench", "--affinities", "101"],
                "--affinities must contain percentages from 0 through 100",
            ),
            (
                &["perfbench", "--shared-pct", "101"],
                "--shared-pct must be a percentage from 0 through 100",
            ),
            (
                &["perfbench", "--workers-per-shape", "10,126"],
                "--num-keys=5000 gives 504 workers stripes of 9 keys, \
                 fewer than --multi-keys=10",
            ),
            (
                &["perfbench", "--quiet-period", "0s"],
                "--quiet-period must be greater than zero",
            ),
            (
                &["perfbench", "--quiet-period", "2s", "--seed-timeout", "1s"],
                "--seed-timeout must be at least --quiet-period",
            ),
            (
                &["perfbench", "--modes", "medium"],
                "unknown mode \"medium\" (expected lo|hi)",
            ),
            (
                &["perfbench", "--workers-per-shape", "0", "--modes", "medium"],
                "--workers-per-shape must contain values >= 1",
            ),
        ];

        for (args, expected) in cases {
            let cli = TestCli::try_parse_from(*args)?;
            let error = match cli.options.cell_dimensions() {
                Ok(_) => panic!("configuration should be rejected: {args:?}"),
                Err(error) => error,
            };
            assert_eq!(
                error.to_string(),
                *expected,
                "unexpected validation precedence for {args:?}"
            );
        }
        Ok(())
    }
}
