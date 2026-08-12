use std::error::Error;
use std::time::Duration;

use clap::Args;

#[derive(Clone, Args)]
pub(crate) struct Options {
    /// Minimum measured window per cell. The cell keeps running all shapes
    /// concurrently past this until every shape's throughput estimate reaches
    /// `--target-ci`, or `--max-duration` is hit.
    #[arg(long, default_value = "2s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) duration: Duration,
    /// Upper bound on a cell's measured window: the cell stops here even if a
    /// shape (typically a heavily-contended write shape) has not yet reached
    /// `--target-ci`; such a shape's result is flagged not-converged.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) max_duration: Duration,
    /// Target relative half-width of each shape's throughput 95% confidence
    /// interval (`0.1` = +/-10%). The cell runs until every shape reaches it or
    /// `--max-duration`. `0` disables adaptivity: run exactly `--duration`.
    #[arg(long, default_value_t = 0.1)]
    pub(super) target_ci: f64,
    /// Total concurrent workers per shape, split as evenly as possible across
    /// the Databases.
    #[arg(long, default_value_t = 8)]
    pub(super) workers_per_shape: usize,
    /// Client `Database`s in each cell. Every Database runs every shape and has
    /// a distinct home collection. Must not exceed `--workers-per-shape`.
    #[arg(long, default_value_t = 4)]
    pub(super) databases: usize,
    /// Keys touched by the multi-key shapes (`rwMany`, `roMulti`); clamped to
    /// the pool size in the `hi` mode.
    #[arg(long, default_value_t = 10)]
    pub(super) multi_keys: usize,
    /// Key-pool size per collection for the `lo` (spread) mode. A few thousand
    /// keys already make same-key overlap rare; larger pools add seeding cost
    /// without materially lowering contention further.
    #[arg(long, default_value_t = 5000)]
    pub(super) num_keys: usize,
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
    /// Required time with no change to the setup Database's completed-split
    /// counter before measurement clients are opened. This is real wall time;
    /// model-time acceleration also compresses the background split cadence.
    #[arg(long, default_value = "10s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) split_quiet: Duration,
    /// Maximum wall time allowed for setup splits to become quiet.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    pub(super) split_settle_timeout: Duration,
}

impl Options {
    /// Validates the mixed-workload configuration and enumerates its cells.
    pub(super) fn cell_dimensions(&self) -> Result<Vec<CellDimension>, Box<dyn Error>> {
        self.validate()?;
        let modes = parse_modes(&self.modes)?;
        Ok(modes
            .into_iter()
            .flat_map(|mode| {
                self.affinities
                    .iter()
                    .copied()
                    .map(move |affinity_pct| CellDimension { mode, affinity_pct })
            })
            .collect())
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.workers_per_shape == 0 {
            return Err("--workers-per-shape must be >= 1".into());
        }
        if self.databases == 0 || self.databases > self.workers_per_shape {
            return Err("--databases must be between 1 and --workers-per-shape".into());
        }
        if self.affinities.is_empty() || self.affinities.iter().any(|&a| a > 100) {
            return Err("--affinities must contain percentages from 0 through 100".into());
        }
        if self.split_quiet.is_zero() {
            return Err("--split-quiet must be greater than zero".into());
        }
        if self.split_settle_timeout < self.split_quiet {
            return Err("--split-settle-timeout must be at least --split-quiet".into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct CellDimension {
    pub(super) mode: Mode,
    pub(super) affinity_pct: u8,
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
            .map(|cell| format!("{}:{}", cell.mode.label(), cell.affinity_pct))
            .collect::<Vec<_>>()
            .join("\n"))
    }

    #[test]
    fn cli_arguments_enumerate_stable_cell_snapshots() -> Result<(), Box<dyn Error>> {
        let cases: &[(&[&str], &str)] = &[
            (
                &["perfbench"],
                "lo:0\nlo:25\nlo:50\nlo:75\nlo:100\nhi:0\nhi:25\nhi:50\nhi:75\nhi:100",
            ),
            (
                &["perfbench", "--modes", "hi,lo", "--affinities", "100,25"],
                "hi:100\nhi:25\nlo:100\nlo:25",
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
                "--workers-per-shape must be >= 1",
            ),
            (
                &["perfbench", "--databases", "0"],
                "--databases must be between 1 and --workers-per-shape",
            ),
            (
                &["perfbench", "--workers-per-shape", "2", "--databases", "3"],
                "--databases must be between 1 and --workers-per-shape",
            ),
            (
                &["perfbench", "--affinities", "101"],
                "--affinities must contain percentages from 0 through 100",
            ),
            (
                &["perfbench", "--split-quiet", "0s"],
                "--split-quiet must be greater than zero",
            ),
            (
                &[
                    "perfbench",
                    "--split-quiet",
                    "2s",
                    "--split-settle-timeout",
                    "1s",
                ],
                "--split-settle-timeout must be at least --split-quiet",
            ),
            (
                &["perfbench", "--modes", "medium"],
                "unknown mode \"medium\" (expected lo|hi)",
            ),
            (
                &["perfbench", "--workers-per-shape", "0", "--modes", "medium"],
                "--workers-per-shape must be >= 1",
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
