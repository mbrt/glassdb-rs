//! Scenario parts: the kinds, end conditions, and axis generators that only some
//! experiments use. Each part is in the file of its scenario, and this module
//! registers it by name.

mod contention;
mod diagnostics;
mod inline_pressure;

use std::error::Error;
use std::sync::Arc;

use toml::Value;

use super::runner::kinds::Kind;
use super::runner::until::PassCondition;

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
