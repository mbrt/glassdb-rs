//! The kind of the `diagnostics` experiment that reads on a new database
//! instance.

use std::error::Error;

use async_trait::async_trait;
use glassdb::Error as GError;
use serde::Deserialize;
use toml::Value;

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
