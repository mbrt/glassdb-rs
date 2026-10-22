//! Key sources: how a transaction selects its keys from a pool.

use std::collections::HashSet;
use std::error::Error;

use rand::RngExt;
use rand::rngs::StdRng;
use serde::Deserialize;
use toml::Value;

use super::config::typed;

/// A key source with its parameters.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SourceSpec {
    /// The worker's own keys, every Nth key of the pool, and with
    /// `shared_pct` percent of the transactions any key of the pool.
    Stripe { shared_pct: u8 },
    /// Any keys of the pool.
    Pool,
    /// Consecutive keys of the worker's own block of the pool.
    Adjacent,
    /// Always these keys.
    Index { keys: Vec<usize> },
    /// One key at a time, in a fixed order. One pass is all of them.
    Sequence {
        start: usize,
        count: usize,
        order: Order,
    },
    /// The first `shared` keys of the pool, then the worker's own block.
    Overlap { shared: usize },
}

/// The sources whose workers divide one pool among them. Workers of other
/// families do not take a part of the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Family {
    /// `stripe` and `adjacent`: each worker owns a distinct part.
    Owned,
    /// `overlap`: each worker owns a block after the shared keys.
    Overlap,
    /// The sources that own no keys.
    Free,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Order {
    Ascending,
    /// Alternates between the lower half and the upper half.
    AlternateHalves,
}

/// The keys of one worker, and how it selects the keys of each transaction.
pub(crate) struct WorkerKeys {
    spec: SourceSpec,
    pool_size: usize,
    /// The index of the worker among the workers of its family on the pool.
    slot: usize,
    /// The workers of its family on the pool.
    slots: usize,
    count: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct StripeParams {
    #[serde(default = "default_shared_pct")]
    shared_pct: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexParams {
    keys: Vec<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SequenceParams {
    start: usize,
    count: usize,
    #[serde(default = "default_order")]
    order: Order,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OverlapParams {
    shared: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoParams {}

impl SourceSpec {
    /// Parses a source: a name, or a table with one name and its parameters.
    /// No value is the default, `stripe`.
    pub(crate) fn from_value(value: Option<&Value>) -> Result<Self, Box<dyn Error>> {
        let (name, params) = match value {
            None => ("stripe".to_string(), Value::Table(Default::default())),
            Some(Value::String(name)) => (name.clone(), Value::Table(Default::default())),
            Some(Value::Table(table)) if table.len() == 1 => {
                let (name, params) = table.iter().next().expect("one entry");
                (name.clone(), params.clone())
            }
            Some(other) => {
                return Err(format!(
                    "`source` must be a name or a table with one name, got {other}"
                )
                .into());
            }
        };
        let what = format!("source {name}");
        Ok(match name.as_str() {
            "stripe" => {
                let params: StripeParams = typed(&params, &what)?;
                if params.shared_pct > 100 {
                    return Err("shared-pct must be a percentage from 0 through 100".into());
                }
                Self::Stripe {
                    shared_pct: params.shared_pct,
                }
            }
            "pool" => {
                let NoParams {} = typed(&params, &what)?;
                Self::Pool
            }
            "adjacent" => {
                let NoParams {} = typed(&params, &what)?;
                Self::Adjacent
            }
            "index" => {
                let params: IndexParams = typed(&params, &what)?;
                Self::Index { keys: params.keys }
            }
            "sequence" => {
                let params: SequenceParams = typed(&params, &what)?;
                Self::Sequence {
                    start: params.start,
                    count: params.count,
                    order: params.order,
                }
            }
            "overlap" => {
                let params: OverlapParams = typed(&params, &what)?;
                Self::Overlap {
                    shared: params.shared,
                }
            }
            other => return Err(format!("unknown key source {other:?}").into()),
        })
    }

    pub(crate) fn family(&self) -> Family {
        match self {
            Self::Stripe { .. } | Self::Adjacent => Family::Owned,
            Self::Overlap { .. } => Family::Overlap,
            Self::Pool | Self::Index { .. } | Self::Sequence { .. } => Family::Free,
        }
    }

    /// Returns the pool size that this source needs, for a pool without
    /// `keys`, or none if the source cannot give one.
    pub(crate) fn pool_size(&self, keys: usize, workers: usize) -> Option<usize> {
        match self {
            Self::Index { keys } => keys.iter().max().map(|max| max + 1),
            Self::Sequence { start, count, .. } => Some(start + count),
            Self::Overlap { shared } => Some(shared + workers * keys.saturating_sub(*shared)),
            Self::Stripe { .. } | Self::Pool | Self::Adjacent => None,
        }
    }

    /// Checks that a pool of `pool` keys gives each of `workers` workers the
    /// keys of a transaction of `keys` keys.
    pub(crate) fn check(
        &self,
        pool: usize,
        keys: usize,
        workers: usize,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            Self::Stripe { .. } | Self::Adjacent => {
                let own = pool / workers.max(1);
                if workers > 0 && own < keys {
                    return Err(format!(
                        "a pool of {pool} keys gives {workers} workers {own} own keys each, \
                         fewer than the {keys} keys of a transaction"
                    )
                    .into());
                }
            }
            Self::Pool => {}
            Self::Index { keys: indices } => {
                if let Some(&max) = indices.iter().max().filter(|&&max| max >= pool) {
                    return Err(format!("index key {max} is outside a pool of {pool} keys").into());
                }
                if indices.len() != keys {
                    return Err(format!(
                        "the index source has {} keys, but a transaction has {keys}",
                        indices.len()
                    )
                    .into());
                }
            }
            Self::Sequence { start, count, .. } => {
                if start + count > pool || *count == 0 {
                    return Err(format!("the sequence is outside a pool of {pool} keys").into());
                }
                if keys != 1 {
                    return Err("a sequence gives one key to each transaction".into());
                }
            }
            Self::Overlap { shared } => {
                if *shared == 0 || *shared > keys {
                    return Err(format!("shared must be from 1 through keys ({keys})").into());
                }
                if shared + workers * (keys - shared) > pool {
                    return Err(format!("the overlap does not fit a pool of {pool} keys").into());
                }
            }
        }
        Ok(())
    }

    /// Returns the keys of one worker, which is the `slot`th of `slots`
    /// workers of its family on a pool of `pool_size` keys.
    pub(crate) fn worker(
        &self,
        pool_size: usize,
        slot: usize,
        slots: usize,
        count: usize,
    ) -> WorkerKeys {
        WorkerKeys {
            spec: self.clone(),
            pool_size,
            slot,
            slots: slots.max(1),
            count: count.max(1),
        }
    }
}

impl WorkerKeys {
    /// Returns the pool indices of transaction `round` of the worker, in
    /// ascending order except for a sequence.
    pub(crate) fn select(&self, rng: &mut StdRng, round: u64) -> Vec<usize> {
        match &self.spec {
            SourceSpec::Stripe { shared_pct } => {
                // Draw at every percentage, so that cells with other
                // percentages keep the same later draws.
                let shared = rng.random_range(0..100u8) < *shared_pct;
                if shared {
                    pick(rng, self.pool_size, 0, 1, self.count)
                } else {
                    pick(rng, self.pool_size, self.slot, self.slots, self.count)
                }
            }
            SourceSpec::Pool => pick(rng, self.pool_size, 0, 1, self.count),
            SourceSpec::Adjacent => {
                let block = self.pool_size / self.slots;
                let start = self.slot * block;
                let count = self.count.min(block).max(1);
                let first = start + rng.random_range(0..=block - count);
                (first..first + count).collect()
            }
            SourceSpec::Index { keys } => keys.clone(),
            SourceSpec::Sequence {
                start,
                count,
                order,
            } => {
                let position = (round % *count as u64) as usize;
                let offset = match order {
                    Order::Ascending => position,
                    Order::AlternateHalves => {
                        let half = count / 2;
                        if position.is_multiple_of(2) {
                            position / 2
                        } else {
                            half + position / 2
                        }
                    }
                };
                vec![start + offset]
            }
            SourceSpec::Overlap { shared } => {
                let own = self.count - shared;
                let block = shared + self.slot * own;
                (0..*shared).chain(block..block + own).collect()
            }
        }
    }

    /// Returns the transactions of one pass of a sequence.
    pub(crate) fn pass_len(&self) -> Option<u64> {
        match &self.spec {
            SourceSpec::Sequence { count, .. } => Some(*count as u64),
            _ => None,
        }
    }
}

/// Picks `count` distinct indices of the stripe `offset + k * stride`.
///
/// A stripe with fewer members than `count` gives all its members.
fn pick(
    rng: &mut StdRng,
    pool_size: usize,
    offset: usize,
    stride: usize,
    count: usize,
) -> Vec<usize> {
    let members = pool_size.saturating_sub(offset).div_ceil(stride);
    let count = count.min(members).max(1);
    let index = |position: usize| position * stride + offset;
    if count >= members {
        return (0..members).map(index).collect();
    }
    let mut selected = HashSet::with_capacity(count);
    while selected.len() < count {
        selected.insert(rng.random_range(0..members));
    }
    let mut selected: Vec<_> = selected.into_iter().map(index).collect();
    selected.sort_unstable();
    selected
}

fn default_shared_pct() -> u8 {
    1
}

fn default_order() -> Order {
    Order::Ascending
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;

    use super::*;

    fn source(text: &str) -> SourceSpec {
        let table: toml::value::Table = toml::from_str(&format!("source = {text}")).unwrap();
        SourceSpec::from_value(table.get("source")).unwrap()
    }

    #[test]
    fn stripes_keep_workers_apart_unless_shared() {
        let spec = source("{ stripe = { shared-pct = 0 } }");
        let mut rng = StdRng::seed_from_u64(1);
        let keys: Vec<HashSet<usize>> = (0..4)
            .map(|slot| {
                let worker = spec.worker(40, slot, 4, 3);
                (0..50)
                    .flat_map(|round| worker.select(&mut rng, round))
                    .collect()
            })
            .collect();
        for (index, own) in keys.iter().enumerate() {
            assert!(own.iter().all(|key| key % 4 == index));
            for other in &keys[index + 1..] {
                assert!(own.is_disjoint(other));
            }
        }
    }

    #[test]
    fn adjacent_keys_are_consecutive_in_the_block_of_the_worker() {
        let worker = source("\"adjacent\"").worker(100, 2, 4, 3);
        let mut rng = StdRng::seed_from_u64(2);
        for round in 0..50 {
            let keys = worker.select(&mut rng, round);
            assert_eq!(keys.len(), 3);
            assert!(keys.windows(2).all(|pair| pair[1] == pair[0] + 1));
            assert!(keys.iter().all(|key| (50..75).contains(key)));
        }
    }

    #[test]
    fn a_sequence_alternates_between_its_halves() {
        let worker =
            source("{ sequence = { start = 4, count = 6, order = \"alternate-halves\" } }")
                .worker(10, 0, 1, 1);
        let mut rng = StdRng::seed_from_u64(3);
        let keys: Vec<_> = (0..6)
            .flat_map(|round| worker.select(&mut rng, round))
            .collect();
        assert_eq!(keys, [4, 7, 5, 8, 6, 9]);
        assert_eq!(worker.pass_len(), Some(6));
    }

    #[test]
    fn overlapping_workers_share_the_first_keys_and_own_the_rest() {
        let spec = source("{ overlap = { shared = 2 } }");
        assert_eq!(spec.pool_size(4, 3), Some(8));
        let mut rng = StdRng::seed_from_u64(4);
        assert_eq!(spec.worker(8, 0, 3, 4).select(&mut rng, 0), [0, 1, 2, 3]);
        assert_eq!(spec.worker(8, 2, 3, 4).select(&mut rng, 0), [0, 1, 6, 7]);
    }

    #[test]
    fn invalid_sources_are_rejected() {
        assert!(source("{ stripe = {} }").check(10, 4, 5).is_err());
        assert!(
            source("{ overlap = { shared = 3 } }")
                .check(10, 2, 1)
                .is_err()
        );
        assert!(source("{ index = { keys = [5] } }").check(5, 1, 1).is_err());
        let table: toml::value::Table = toml::from_str("source = { pool = { x = 1 } }").unwrap();
        assert!(SourceSpec::from_value(table.get("source")).is_err());
    }
}
