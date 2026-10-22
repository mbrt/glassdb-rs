//! End conditions of a measurement.

use std::error::Error;
use std::sync::Arc;

use glassdb::Stats;
use toml::Value;

/// When the measurement of a cell ends.
#[derive(Clone)]
pub(crate) enum Until {
    /// Every shape has its commits in each throughput window on average, after
    /// the minimum duration.
    Converged,
    /// Exactly the measurement duration.
    Duration,
    /// Each worker with a `sequence` source makes one pass.
    Passes,
    /// Each worker with a `sequence` source makes passes until one pass meets
    /// the condition.
    Pass(Arc<dyn PassCondition>),
}

/// A condition on one pass of a worker with a `sequence` source.
pub(crate) trait PassCondition: Send + Sync {
    /// Reports whether the pass with the stats `pass` of its database instance
    /// ends the measurement of its worker.
    fn ends(&self, pass: &Stats) -> bool;

    /// Whether the pass that ends the measurement is outside it, so that the
    /// measurement has only the passes that did not meet the condition.
    fn excludes_last_pass(&self) -> bool {
        false
    }
}

impl Until {
    /// Parses an end condition by name.
    pub(crate) fn from_value(value: &Value) -> Result<Self, Box<dyn Error>> {
        let name = value
            .as_str()
            .ok_or_else(|| format!("cell.measure.until must be a name, got {value}"))?;
        Ok(match name {
            "converged" => Self::Converged,
            "duration" => Self::Duration,
            "passes" => Self::Passes,
            other => match crate::scenarios::pass_condition(other) {
                Some(condition) => Self::Pass(condition),
                None => return Err(format!("unknown end condition {other:?}").into()),
            },
        })
    }

    /// Reports whether the workers with a `sequence` source decide the end.
    pub(crate) fn by_passes(&self) -> bool {
        matches!(self, Self::Passes | Self::Pass(_))
    }

    /// Reports whether the pass that ends the measurement is outside it.
    pub(crate) fn excludes_last_pass(&self) -> bool {
        matches!(self, Self::Pass(condition) if condition.excludes_last_pass())
    }
}
