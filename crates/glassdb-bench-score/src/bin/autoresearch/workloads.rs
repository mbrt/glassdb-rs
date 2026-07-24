//! The fixed suite of single-client workloads, ported from the Go
//! `hack/autoresearch/bench/workloads.go`. Iteration counts are deliberately
//! constant so the primary score stays comparable across runs and machines.
//!
//! Each workload does its setup (collection creation, seeding) outside the
//! measured region, then brackets exactly the measured transactions with
//! [`Measure::begin`]/[`Measure::end`]. Go's `ReadMulti` becomes a parallel
//! `futures::future::join_all` of `tx.read`, matching the rest of the repo's
//! benchmarks.
//!
//! This file is part of the autoresearch fixed infrastructure: it defines the
//! measured workloads and must NOT be modified by autoresearch experiments.

use futures::future::join_all;

use glassdb::{Collection, Database, Error};

use crate::metrics::{Measure, Sample};

// Fixed iteration counts (identical to the Go harness).
const SINGLE_RMW_TX: usize = 200;
const MULTI_RMW_TX: usize = 100;
const MULTI_RMW_KEYS: usize = 10;
const BATCH_READ_TX: usize = 200;
const BATCH_READ_KEYS: usize = 10;
const BATCH_WRITE_TX: usize = 50;
const BATCH_WRITE_KEYS: usize = 100;
const READ_REPEAT_TX: usize = 200;

/// The workload names, in suite order.
pub const NAMES: &[&str] = &[
    "singleRMW",
    "multiRMW10",
    "batchRead10",
    "batchWrite100",
    "readRepeat",
];

/// Runs the named workload against a fresh `db` and returns its measurement.
pub async fn run(name: &str, db: &Database) -> Result<Sample, Error> {
    match name {
        "singleRMW" => single_rmw(db).await,
        "multiRMW10" => multi_rmw(db).await,
        "batchRead10" => batch_read(db).await,
        "batchWrite100" => batch_write(db).await,
        "readRepeat" => read_repeat(db).await,
        other => Err(Error::InvalidInput(format!("unknown workload {other:?}"))),
    }
}

// --- Value codec (self-contained; only op counts feed the score) ----------

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    if b.len() < 8 {
        return 0;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&b[..8]);
    i64::from_le_bytes(arr)
}

// --- Setup helpers --------------------------------------------------------

async fn create_coll(db: &Database, name: &str) -> Result<Collection, Error> {
    db.root_collection()
        .create_collection_if_absent(name.as_bytes())
        .await
}

fn make_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("key{i}").into_bytes()).collect()
}

// --- Workloads ------------------------------------------------------------

/// 200 read-modify-write transactions on a single key.
async fn single_rmw(db: &Database) -> Result<Sample, Error> {
    let coll = create_coll(db, "single").await?;
    let coll = &coll;
    let key: &[u8] = b"k";

    let mut m = Measure::new("singleRMW");
    m.begin(db);
    for _ in 0..SINGLE_RMW_TX {
        db.tx(|tx| async move {
            let v = match tx.read(coll, key).await {
                Ok(Some(v)) => v,
                Ok(None) => Vec::new(),
                Err(e) => return Err(e),
            };
            tx.write(coll, key, &write_int(read_int(&v) + 1))
        })
        .await?;
    }
    m.end(db);
    Ok(m.into_sample())
}

/// 100 transactions that read 10 keys in parallel and write each one back.
async fn multi_rmw(db: &Database) -> Result<Sample, Error> {
    let coll = create_coll(db, "multi").await?;
    let keys = make_keys(MULTI_RMW_KEYS);
    let coll = &coll;
    let keys = &keys;

    let mut m = Measure::new("multiRMW10");
    m.begin(db);
    for _ in 0..MULTI_RMW_TX {
        db.tx(|tx| async move {
            let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            for (k, rv) in keys.iter().zip(vals) {
                let v = match rv {
                    Ok(Some(v)) => v,
                    Ok(None) => Vec::new(),
                    Err(e) => return Err(e),
                };
                tx.write(coll, k, &write_int(read_int(&v) + 1))?;
            }
            Ok(())
        })
        .await?;
    }
    m.end(db);
    Ok(m.into_sample())
}

/// 200 read-only transactions over 10 pre-seeded keys (read in parallel).
async fn batch_read(db: &Database) -> Result<Sample, Error> {
    let coll = create_coll(db, "bread").await?;
    let keys = make_keys(BATCH_READ_KEYS);

    // Seed the keys (unmeasured).
    {
        let coll = &coll;
        let keys = &keys;
        db.tx(|tx| async move {
            for (i, k) in keys.iter().enumerate() {
                tx.write(coll, k, &write_int(i as i64))?;
            }
            Ok(())
        })
        .await?;
    }

    let coll = &coll;
    let keys = &keys;
    let mut m = Measure::new("batchRead10");
    m.begin(db);
    for _ in 0..BATCH_READ_TX {
        db.tx(|tx| async move {
            let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            for rv in vals {
                rv?;
            }
            Ok(())
        })
        .await?;
    }
    m.end(db);
    Ok(m.into_sample())
}

/// 50 transactions that each write 100 distinct keys.
async fn batch_write(db: &Database) -> Result<Sample, Error> {
    let coll = create_coll(db, "bwrite").await?;
    let coll = &coll;

    let mut m = Measure::new("batchWrite100");
    m.begin(db);
    for i in 0..BATCH_WRITE_TX {
        db.tx(|tx| async move {
            for j in 0..BATCH_WRITE_KEYS {
                let k = format!("k{}", i * BATCH_WRITE_KEYS + j);
                tx.write(coll, k.as_bytes(), &write_int(j as i64))?;
            }
            Ok(())
        })
        .await?;
    }
    m.end(db);
    Ok(m.into_sample())
}

/// 200 transactions that repeatedly read the same pre-seeded key.
async fn read_repeat(db: &Database) -> Result<Sample, Error> {
    let coll = create_coll(db, "rrepeat").await?;
    let key: &[u8] = b"k";

    // Seed the key (unmeasured).
    {
        let coll = &coll;
        db.tx(|tx| async move { tx.write(coll, key, &write_int(42)) })
            .await?;
    }

    let coll = &coll;
    let mut m = Measure::new("readRepeat");
    m.begin(db);
    for _ in 0..READ_REPEAT_TX {
        db.tx(|tx| async move { tx.read(coll, key).await.map(|_| ()) })
            .await?;
    }
    m.end(db);
    Ok(m.into_sample())
}
