//! Kinds: what one transaction of a shape does.

use std::error::Error;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use futures::future::join_all;
use glassdb::{Collection, Database, Error as GError, KeyScan, StaleDatabaseView};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use toml::Value;
use toml::value::Table;

use super::model::PoolSpec;

/// The units of CPU work of each key that a transaction writes, from the wall
/// time of a measurement in virtual time over its transactions.
const WRITE_CPU_UNITS: usize = 20;

/// The inputs of one transaction.
pub(crate) struct Op<'a> {
    pub(crate) db: &'a Database,
    /// The collections of the transaction, from the selected one on.
    pub(crate) collections: &'a [Collection],
    pub(crate) keys: &'a [Vec<u8>],
    pub(crate) pool: &'a PoolSpec,
    /// The transactions of the worker before this one in the cell.
    pub(crate) round: u64,
    /// The stale view of the worker on `db`, which a kind creates in its
    /// first transaction and uses in each later one on the same instance.
    pub(crate) view: &'a OnceLock<StaleDatabaseView>,
}

/// What one transaction of a shape does, with the parameters of the kind.
#[async_trait]
pub(crate) trait Kind: Send + Sync {
    /// The keys of one transaction.
    fn keys(&self) -> usize;

    /// The units of CPU work of one transaction on `pool`, which virtual
    /// time charges in model time (`TransactionCpu`). The default is one for
    /// each key, as a read of a key costs. A first-order approximation is
    /// enough.
    fn cpu_units(&self, _pool: &PoolSpec) -> usize {
        self.keys()
    }

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
        "read" => Arc::new(Read::parse(&params, name)?),
        "write" => Arc::new(parse::<Write>(&params, name)?),
        "scan" => Arc::new(parse::<Scan>(&params, name)?),
        "create-delete" => Arc::new(parse::<CreateDelete>(&params, name)?),
        "delete" => Arc::new(parse::<Delete>(&params, name)?),
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
    /// Open each collection by name in the transaction body, so that the
    /// transaction also reads the directory of the root collection.
    #[serde(default)]
    open_in_body: bool,
}

/// A serializable read of `keys` keys.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct Read {
    keys: usize,
    /// Run a stale read-only transaction with this bound (ADR-097), in place
    /// of `Database::tx`.
    #[serde(default, deserialize_with = "optional_duration")]
    staleness: Option<std::time::Duration>,
    /// The stale view that runs each stale read-only transaction. The
    /// default is a new view for each transaction.
    #[serde(default)]
    view: Option<View>,
}

/// The stale view of a stale read-only transaction.
#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
enum View {
    /// A new view for each transaction.
    Call,
    /// One view for all transactions of a worker.
    Worker,
}

impl Read {
    fn parse(params: &Value, name: &str) -> Result<Self, Box<dyn Error>> {
        let read: Self = parse(params, name)?;
        if read.view.is_some() && read.staleness.is_none() {
            return Err(format!("kind {name}: view needs staleness").into());
        }
        Ok(read)
    }
}

/// Keeps the values of a read from being optimized away.
fn consume(values: Vec<Result<Option<Vec<u8>>, GError>>) -> Result<(), GError> {
    for value in values {
        std::hint::black_box(value?);
    }
    Ok(())
}

fn optional_duration<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<std::time::Duration>, D::Error> {
    super::config::duration(deserializer).map(Some)
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

/// Deletes the selected key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct Delete {}

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

    fn cpu_units(&self, _pool: &PoolSpec) -> usize {
        WRITE_CPU_UNITS * self.keys * self.collections
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
        let names: Vec<&[u8]> = if self.open_in_body {
            op.collections
                .iter()
                .map(|collection| {
                    collection.name().ok_or_else(|| {
                        GError::InvalidInput("open-in-body needs named collections".into())
                    })
                })
                .collect::<Result<_, _>>()?
        } else {
            Vec::new()
        };
        let names = &names;
        op.db
            .tx(|tx| async move {
                let root = tx.root_collection();
                for name in names {
                    tx.open_collection(&root, name).await?;
                }
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
        match self.staleness {
            Some(staleness) => {
                let body = |tx: glassdb::ReadTransaction| async move {
                    consume(join_all(op.keys.iter().map(|key| tx.read(collection, key))).await)
                };
                match self.view.unwrap_or(View::Call) {
                    View::Call => op.db.stale(staleness).read_tx(body).await,
                    View::Worker => {
                        let view = op.view.get_or_init(|| op.db.stale(staleness));
                        view.read_tx(body).await
                    }
                }
            }
            None => {
                op.db
                    .tx(|tx| async move {
                        consume(join_all(op.keys.iter().map(|key| tx.read(collection, key))).await)
                    })
                    .await
            }
        }
    }
}

#[async_trait]
impl Kind for Write {
    fn keys(&self) -> usize {
        1
    }

    fn cpu_units(&self, _pool: &PoolSpec) -> usize {
        WRITE_CPU_UNITS
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        let size = self.value_bytes.unwrap_or(op.pool.value_bytes);
        op.collections[0]
            .write(&op.keys[0], &round_value(op.round, size))
            .await
    }
}

#[async_trait]
impl Kind for Delete {
    fn keys(&self) -> usize {
        1
    }

    async fn run(&self, op: &Op<'_>) -> Result<(), GError> {
        op.collections[0].delete(&op.keys[0]).await
    }
}

#[async_trait]
impl Kind for Scan {
    fn keys(&self) -> usize {
        1
    }

    /// One unit for each key of the span, or of the pool for a scan of the
    /// whole collection.
    fn cpu_units(&self, pool: &PoolSpec) -> usize {
        self.span.or(pool.keys).unwrap_or(1)
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

    fn cpu_units(&self, _pool: &PoolSpec) -> usize {
        WRITE_CPU_UNITS
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
    fn a_view_of_a_read_needs_staleness() {
        let mut params = Table::new();
        params.insert("keys".into(), Value::Integer(1));
        for view in ["call", "worker"] {
            params.insert("view".into(), Value::String(view.into()));
            let error = build("read", params.clone()).err().unwrap().to_string();
            assert!(error.contains("view needs staleness"), "{view}: {error}");
        }
        params.insert("staleness".into(), Value::String("5s".into()));
        assert!(build("read", params).is_ok());
    }

    #[test]
    fn a_read_modify_write_changes_the_value_and_keeps_its_size() {
        assert_eq!(next_value(Some(vec![1, 2]), 2), vec![2, 2]);
        assert_eq!(next_value(Some(vec![1]), 3), vec![0x5a; 3]);
        assert_eq!(next_value(None, 2), vec![0x5a; 2]);
    }
}
