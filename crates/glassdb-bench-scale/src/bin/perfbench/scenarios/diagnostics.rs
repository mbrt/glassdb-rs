//! The parts of the `diagnostics` experiment: single transactions with warm
//! caches, and the backend costs of each transaction.

use std::error::Error;

use async_trait::async_trait;
use glassdb::Error as GError;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use toml::Value;

use super::ProbeInput;
use crate::runner::Window;
use crate::runner::kinds::{Kind, Op, parse};

/// Reads keys on a new database instance, which has no cache. The open of the
/// instance and its collections is not in the time of the read.
pub(crate) struct FreshClientRead {
    keys: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FreshClientReadParams {
    keys: usize,
}

impl FreshClientRead {
    pub(crate) fn new(params: &Value) -> Result<Self, Box<dyn Error>> {
        let params: FreshClientReadParams = parse(params, "fresh-client-read")?;
        Ok(Self { keys: params.keys })
    }
}

#[async_trait]
impl Kind for FreshClientRead {
    fn keys(&self) -> usize {
        self.keys
    }

    fn fresh_instance(&self) -> bool {
        true
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        for key in op.keys {
            let value = op.collections[0]
                .read(key)
                .await?
                .ok_or_else(|| GError::internal("missing diagnostic key"))?;
            std::hint::black_box(value);
        }
        Ok(())
    }
}

/// Returns the backend requests, the body bytes, and the coordinator counters
/// of each transaction, in the windows of the workload, of the shutdown of the
/// instances, and of both.
pub(crate) fn costs(input: &ProbeInput) -> Json {
    let n = input.transactions.max(1) as f64;
    let window = |window: &Window| {
        json!({
            "reads": window.stats.backend.obj_reads as f64 / n,
            "writes": window.stats.backend.obj_writes as f64 / n,
            "lists": window.stats.backend.obj_lists as f64 / n,
            "readBodyBytes": window.breakdown.read_bytes() as f64 / n,
            "writeBodyBytes": window.breakdown.write_bytes() as f64 / n,
            "coordinatorSubmissions": window.stats.coordinator.submissions as f64 / n,
            "coordinatorRounds": window.stats.coordinator.rounds as f64 / n,
        })
    };
    let mut combined = input.workload;
    combined.add(input.shutdown);
    json!({
        "transactions": input.transactions,
        "workload": window(&input.workload),
        "shutdown": window(&input.shutdown),
        "combined": window(&combined),
    })
}
