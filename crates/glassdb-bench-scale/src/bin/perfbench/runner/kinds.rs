//! Kinds: what one transaction of a shape does.

use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::join_all;
use glassdb::{Collection, Database, Error as GError, KeyScan};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use toml::Value;
use toml::value::Table;

use super::model::PoolSpec;

/// The inputs of one transaction.
pub(crate) struct Op<'a> {
    pub(crate) db: &'a Database,
    /// The collections of the transaction, from the selected one on.
    pub(crate) collections: &'a [Collection],
    pub(crate) keys: &'a [Vec<u8>],
    pub(crate) pool: &'a PoolSpec,
    /// The transactions of the worker before this one in the cell.
    pub(crate) round: u64,
}

/// What one transaction of a shape does, with the parameters of the kind.
#[async_trait]
pub(crate) trait Kind: Send + Sync {
    /// The keys of one transaction.
    fn keys(&self) -> usize;

    /// The collections of one transaction.
    fn collections(&self) -> usize {
        1
    }

    /// The consecutive transactions of a worker that use the same keys.
    fn repeats(&self) -> u64 {
        1
    }

    /// Whether each transaction runs on a new database instance. The runner
    /// opens the instance before the transaction and shuts it down after, and
    /// neither is in the time of the transaction.
    fn fresh_instance(&self) -> bool {
        false
    }

    /// Runs one transaction.
    async fn run(&self, op: &Op<'_>) -> Result<(), GError>;
}

/// Returns the kind `name` with the parameters in `params`.
pub(crate) fn build(name: &str, params: Table) -> Result<Arc<dyn Kind>, Box<dyn Error>> {
    let params = Value::Table(params);
    Ok(match name {
        "read-modify-write" => Arc::new(parse::<ReadModifyWrite>(&params, name)?),
        "read" => Arc::new(parse::<Read>(&params, name)?),
        "write" => Arc::new(parse::<Write>(&params, name)?),
        "scan" => Arc::new(parse::<Scan>(&params, name)?),
        "create-delete" => Arc::new(parse::<CreateDelete>(&params, name)?),
        other => match crate::scenarios::kind(other, &params)? {
            Some(kind) => kind,
            None => return Err(format!("unknown kind {other:?}").into()),
        },
    })
}

/// Parses the parameters of a kind, and rejects unknown ones.
pub(crate) fn parse<T: DeserializeOwned>(params: &Value, name: &str) -> Result<T, Box<dyn Error>> {
    params
        .clone()
        .try_into()
        .map_err(|e| format!("kind {name}: {e}").into())
}

/// Reads `keys` keys and writes each of them.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ReadModifyWrite {
    keys: usize,
    /// The size of each written value. The default is the value size of the
    /// pool.
    #[serde(default)]
    value_bytes: Option<usize>,
    #[serde(default = "one")]
    collections: usize,
}

/// A serializable read of `keys` keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct Read {
    keys: usize,
}

/// Writes one key without a read.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct Write {
    #[serde(default)]
    value_bytes: Option<usize>,
}

/// Scans `span` keys from the selected key, or the whole collection without
/// a span.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct Scan {
    #[serde(default)]
    span: Option<usize>,
}

/// Creates the selected key, and deletes it in the next transaction.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct CreateDelete {
    #[serde(default)]
    value_bytes: Option<usize>,
    /// Scan the whole collection first, so that the transaction cannot commit
    /// direct and takes the membership lock of the leaf.
    #[serde(default)]
    locked: bool,
}

#[async_trait]
impl Kind for ReadModifyWrite {
    fn keys(&self) -> usize {
        self.keys
    }

    fn collections(&self) -> usize {
        self.collections
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let size = self.value_bytes.unwrap_or(op.pool.value_bytes);
        let members: Vec<(&Collection, &Vec<u8>)> = op
            .collections
            .iter()
            .flat_map(|collection| op.keys.iter().map(move |key| (collection, key)))
            .collect();
        let members = &members;
        op.db
            .tx(|tx| async move {
                let values = join_all(members.iter().map(|(c, key)| tx.read(c, key))).await;
                for ((collection, key), value) in members.iter().zip(values) {
                    tx.write(collection, key, &next_value(value?, size))?;
                }
                Ok(())
            })
            .await
    }
}

#[async_trait]
impl Kind for Read {
    fn keys(&self) -> usize {
        self.keys
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let collection = &op.collections[0];
        op.db
            .tx(|tx| async move {
                let values = join_all(op.keys.iter().map(|key| tx.read(collection, key))).await;
                for value in values {
                    std::hint::black_box(value?);
                }
                Ok(())
            })
            .await
    }
}

#[async_trait]
impl Kind for Write {
    fn keys(&self) -> usize {
        1
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let size = self.value_bytes.unwrap_or(op.pool.value_bytes);
        op.collections[0]
            .write(&op.keys[0], &round_value(op.round, size))
            .await
    }
}

#[async_trait]
impl Kind for Scan {
    fn keys(&self) -> usize {
        1
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let collection = &op.collections[0];
        let keys = match self.span {
            None => collection.scan_keys(KeyScan::all()).await?,
            Some(span) => {
                let start = &op.keys[0];
                collection
                    .scan_keys(KeyScan::range(start, &[0xff]).limit(span))
                    .await?
            }
        };
        std::hint::black_box(keys);
        Ok(())
    }
}

#[async_trait]
impl Kind for CreateDelete {
    fn keys(&self) -> usize {
        1
    }

    fn repeats(&self) -> u64 {
        2
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let collection = &op.collections[0];
        let key = &op.keys[0];
        let create = op.round.is_multiple_of(2);
        let value = round_value(op.round, self.value_bytes.unwrap_or(op.pool.value_bytes));
        if !self.locked {
            return if create {
                collection.write(key, &value).await
            } else {
                collection.delete(key).await
            };
        }
        let value = &value;
        op.db
            .tx(|tx| async move {
                tx.scan_keys(collection, KeyScan::all()).await?;
                if create {
                    tx.write(collection, key, value)
                } else {
                    tx.delete(collection, key)
                }
            })
            .await
    }
}

/// Returns the value that a read-modify-write stores: the read value with its
/// first byte changed, or a new value when the key has none of `size` bytes.
fn next_value(read: Option<Vec<u8>>, size: usize) -> Vec<u8> {
    match read {
        Some(mut value) if value.len() == size && size > 0 => {
            value[0] = value[0].wrapping_add(1);
            value
        }
        _ => vec![0x5a; size],
    }
}

/// Returns a value of `size` bytes that starts with the round number.
fn round_value(round: u64, size: usize) -> Vec<u8> {
    let mut value = vec![0x5a; size];
    let bytes = round.to_le_bytes();
    let n = bytes.len().min(size);
    value[..n].copy_from_slice(&bytes[..n]);
    value
}

fn one() -> usize {
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_parameters_of_a_kind_are_rejected() {
        let mut params = Table::new();
        params.insert("keys".into(), Value::Integer(1));
        params.insert("span".into(), Value::Integer(4));
        let error = build("read-modify-write", params)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("span"), "{error}");
        assert!(build("teleport", Table::new()).is_err());
    }

    #[test]
    fn a_read_modify_write_changes_the_value_and_keeps_its_size() {
        assert_eq!(next_value(Some(vec![1, 2]), 2), vec![2, 2]);
        assert_eq!(next_value(Some(vec![1]), 3), vec![0x5a; 3]);
        assert_eq!(next_value(None, 2), vec![0x5a; 2]);
    }
}
