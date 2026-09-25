//! Topology workload selection and execution.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::future::{join_all, try_join_all};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{RngExt, SeedableRng};
use tokio::task::JoinHandle;

use glassdb::{Collection, Database, Error as GError, KeyScan, ensure_tx};
use glassdb_bench_scale::bench::Bench;
use glassdb_bench_scale::run::split_workers;

/// The transaction shape that one cell measures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Workload {
    Single,
    Hot,
    Adjacent,
    Random,
    Scan,
}

impl Workload {
    pub(super) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "single" => Ok(Workload::Single),
            "hot" => Ok(Workload::Hot),
            "adjacent" => Ok(Workload::Adjacent),
            "random" => Ok(Workload::Random),
            "scan" => Ok(Workload::Scan),
            other => Err(format!("unknown workload {other:?}")),
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Workload::Single => "single",
            Workload::Hot => "hot",
            Workload::Adjacent => "adjacent",
            Workload::Random => "random",
            Workload::Scan => "scan",
        }
    }
}

/// The seeded key pool and the keys that one transaction touches.
#[derive(Clone, Copy)]
pub(super) struct KeySpace {
    pub(super) num_keys: usize,
    pub(super) hot_keys: usize,
    pub(super) multi_keys: usize,
    pub(super) scan_keys: usize,
    pub(super) value_bytes: usize,
}

impl KeySpace {
    /// Returns the value written on every put.
    pub(super) fn value(self) -> Vec<u8> {
        vec![0x5a; self.value_bytes]
    }

    /// Selects the ascending pool indices that one `workload` transaction
    /// touches.
    fn select(self, workload: Workload, rng: &mut StdRng) -> Vec<usize> {
        match workload {
            Workload::Single => vec![rng.random_range(0..self.num_keys)],
            Workload::Hot => vec![rng.random_range(0..self.hot_keys)],
            Workload::Adjacent => self.consecutive(rng, self.multi_keys),
            Workload::Scan => self.consecutive(rng, self.scan_keys),
            Workload::Random => {
                let mut selected = BTreeSet::new();
                while selected.len() < self.multi_keys {
                    selected.insert(rng.random_range(0..self.num_keys));
                }
                selected.into_iter().collect()
            }
        }
    }

    fn consecutive(self, rng: &mut StdRng, count: usize) -> Vec<usize> {
        let start = rng.random_range(0..=self.num_keys - count);
        (start..start + count).collect()
    }
}

/// The key of one pool index. Zero padding makes key order equal index order,
/// so consecutive indices share leaves.
pub(super) fn key(index: usize) -> Vec<u8> {
    format!("key{index:06}").into_bytes()
}

/// Spawns `workers` workers of `workload`, split evenly across `databases`.
pub(super) fn spawn_workers(
    databases: &[Database],
    collections: &[Collection],
    workers: usize,
    workload: Workload,
    key_space: KeySpace,
    bench: &Arc<Bench>,
    stop: &Arc<AtomicBool>,
) -> Vec<JoinHandle<Result<(), GError>>> {
    let slots = split_workers(workers, databases.len())
        .into_iter()
        .enumerate()
        .flat_map(|(database, count)| std::iter::repeat_n(database, count));
    slots
        .enumerate()
        .map(|(index, database)| {
            let seed = 0x9E37_79B9_7F4A_7C15_u64
                .wrapping_add((index as u64 + 1).wrapping_mul(0x1000_0000_0000_0001));
            tokio::spawn(worker(
                databases[database].clone(),
                collections[database].clone(),
                workload,
                key_space,
                bench.clone(),
                stop.clone(),
                seed,
            ))
        })
        .collect()
}

async fn worker(
    db: Database,
    collection: Collection,
    workload: Workload,
    key_space: KeySpace,
    bench: Arc<Bench>,
    stop: Arc<AtomicBool>,
    seed: u64,
) -> Result<(), GError> {
    let mut rng = StdRng::seed_from_u64(seed);
    let value = key_space.value();
    while !stop.load(Ordering::Relaxed) && !bench.is_finished() {
        // Select keys outside measurement so entropy work stays out of the sample.
        let indices = key_space.select(workload, &mut rng);
        let result = match workload {
            Workload::Scan => {
                let start = key(indices[0]);
                let end = key(indices[indices.len() - 1] + 1);
                bench
                    .measure(|| scan_tx(&db, &collection, &start, &end, indices.len()))
                    .await
            }
            _ => {
                let keys: Vec<_> = indices.iter().map(|&index| key(index)).collect();
                bench
                    .measure(|| rmw_tx(&db, &collection, &keys, &value))
                    .await
            }
        };
        if let Err(err) = result {
            stop.store(true, Ordering::Relaxed);
            return Err(err);
        }
    }
    Ok(())
}

/// Rewrites every key of the pool in its own transaction, so that direct
/// commits publish the values inline, as sustained single-key writes do.
pub(super) async fn inline_values(
    db: &Database,
    collection: &Collection,
    key_space: KeySpace,
    workers: usize,
) -> Result<(), GError> {
    // Shuffled, so that concurrent workers write different leaves.
    let mut order: Vec<usize> = (0..key_space.num_keys).collect();
    order.shuffle(&mut StdRng::seed_from_u64(0x1D1E_5EED));
    let value = key_space.value();
    let (order, value) = (&order, &value);
    try_join_all((0..workers).map(|worker| async move {
        for &index in order.iter().skip(worker).step_by(workers) {
            rmw_tx(db, collection, &[key(index)], value).await?;
        }
        Ok::<(), GError>(())
    }))
    .await?;
    Ok(())
}

/// Reads every key of `keys`, then writes `value` to each of them.
async fn rmw_tx(
    db: &Database,
    collection: &Collection,
    keys: &[Vec<u8>],
    value: &[u8],
) -> Result<(), GError> {
    db.tx(|tx| async move {
        let reads = join_all(keys.iter().map(|key| tx.read(collection, key))).await;
        for (key, read) in keys.iter().zip(reads) {
            read?;
            tx.write(collection, key, value)?;
        }
        Ok(())
    })
    .await
}

async fn scan_tx(
    db: &Database,
    collection: &Collection,
    start: &[u8],
    end: &[u8],
    expected: usize,
) -> Result<(), GError> {
    db.tx(|tx| async move {
        let page = tx
            .scan_keys(collection, KeyScan::range(start, end).limit(expected))
            .await?;
        ensure_tx!(
            page.keys().len() == expected,
            GError::internal(format!(
                "scan listed {} seeded keys, expected {expected}",
                page.keys().len()
            ))
        );
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selections_stay_in_the_pool_and_keep_their_locality() {
        let keys = KeySpace {
            num_keys: 10,
            hot_keys: 3,
            multi_keys: 4,
            scan_keys: 6,
            value_bytes: 1,
        };
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..100 {
            let single = keys.select(Workload::Single, &mut rng);
            assert!(single.len() == 1 && single[0] < 10, "{single:?}");

            let hot = keys.select(Workload::Hot, &mut rng);
            assert!(hot.len() == 1 && hot[0] < 3, "{hot:?}");

            let adjacent = keys.select(Workload::Adjacent, &mut rng);
            let first = adjacent[0];
            assert_eq!(adjacent, (first..first + 4).collect::<Vec<_>>());
            assert!(first + 4 <= 10, "{adjacent:?}");

            let random = keys.select(Workload::Random, &mut rng);
            assert_eq!(random.len(), 4);
            assert!(
                random.windows(2).all(|pair| pair[0] < pair[1]),
                "{random:?}"
            );
            assert!(random.iter().all(|&index| index < 10), "{random:?}");

            let scan = keys.select(Workload::Scan, &mut rng);
            let first = scan[0];
            assert_eq!(scan, (first..first + 6).collect::<Vec<_>>());
            assert!(first + 6 <= 10, "{scan:?}");
        }
        assert!(key(9) < key(10), "key order must follow index order");
    }
}
