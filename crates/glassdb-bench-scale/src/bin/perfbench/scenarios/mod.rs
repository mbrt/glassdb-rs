//! Scenario parts: the kinds, end conditions, and probes that only some
//! experiments use. Each part is in the file of its scenario, and this module
//! registers it by name.

mod contention;
mod diagnostics;
mod inline_pressure;

use std::error::Error;
use std::sync::Arc;

use toml::Value;

use super::runner::Window;
use super::runner::kinds::Kind;
use super::runner::until::PassCondition;

/// The measurements of a cell that a probe can read.
pub(crate) struct ProbeInput {
    /// The transactions of the measurement. The opens of the instances that
    /// workers make for single transactions are not in it.
    pub(crate) workload: Window,
    /// The shutdown of all instances of the cell after the measurement.
    pub(crate) shutdown: Window,
    /// The measured transactions of all shapes.
    pub(crate) transactions: u64,
}

/// Returns the scenario kind `name`, or none if no scenario has it.
pub(crate) fn kind(name: &str, params: &Value) -> Result<Option<Arc<dyn Kind>>, Box<dyn Error>> {
    Ok(match name {
        "fresh-client-read" => Some(Arc::new(diagnostics::FreshClientRead::new(params)?)),
        _ => None,
    })
}

/// Returns the values of an axis that the scenario part `name` gives.
pub(crate) fn axis_values(name: &str, params: &Value) -> Result<Vec<Value>, Box<dyn Error>> {
    match name {
        "contention-overlaps" => contention::overlaps(params),
        other => Err(format!("unknown axis generator {other:?}").into()),
    }
}

/// Returns the scenario end condition `name`, or none if no scenario has it.
pub(crate) fn pass_condition(name: &str) -> Option<Arc<dyn PassCondition>> {
    match name {
        "no-locked-pass" => Some(Arc::new(inline_pressure::NoLockedPass)),
        _ => None,
    }
}

/// Returns the fields of the probe `name`.
pub(crate) fn probe(name: &str, input: &ProbeInput) -> Result<serde_json::Value, Box<dyn Error>> {
    match name {
        "costs" => Ok(diagnostics::costs(input)),
        other => Err(format!("unknown probe {other:?}").into()),
    }
}
