//! Property-based mirror of the Go `FuzzConcurrentTx`. Without the byte-driven
//! scheduler middleware (not yet ported), this instead randomizes the per-key
//! increment counts of two concurrently-running Database instances and checks the
//! serializability invariant: each key's final value equals the total number of
//! successful increments applied to it.
//!
//! This is the fast, normal-build sanity check. The deterministic executor
//! drives the same invariant under `--cfg sim` (see the `fuzz/` crate and
//! `concurrent_sim` integration test), where it controls its own clock, so this
//! file (which builds a `start_paused` tokio runtime directly) is compiled out.
#![cfg(not(sim))]

use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::{Backend, Collection, CollectionPath, Database, Error};
use proptest::prelude::*;

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn try_read_int(b: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(b.try_into().ok()?))
}

fn read_int(b: &[u8]) -> i64 {
    try_read_int(b).expect("integer value has the wrong width")
}

async fn read_int_from_tx(
    tx: &glassdb::Transaction,
    c: &Collection,
    k: &[u8],
) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(Some(v)) => try_read_int(&v)
            .ok_or_else(|| Error::internal(format!("key {k:?} has non-integer value {v:?}"))),
        Ok(None) => Ok(0),
        Err(e) => Err(e),
    }
}

async fn rmw(db: &Database, coll: &Collection, key: &[u8], n: u32) -> Result<(), Error> {
    for _ in 0..n {
        db.tx(|tx| async move {
            let cur = read_int_from_tx(&tx, coll, key).await?;
            let next = cur
                .checked_add(1)
                .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))?;
            tx.write(coll, key, &write_int(next))
        })
        .await?;
    }
    Ok(())
}

async fn multi_rmw(
    db: &Database,
    coll: &Collection,
    a: &[u8],
    b: &[u8],
    n: u32,
) -> Result<(), Error> {
    for _ in 0..n {
        db.tx(|tx| async move {
            let va = read_int_from_tx(&tx, coll, a).await?;
            let vb = read_int_from_tx(&tx, coll, b).await?;
            let next_a = va
                .checked_add(1)
                .ok_or_else(|| Error::internal(format!("integer overflow for key {a:?}")))?;
            let next_b = vb
                .checked_add(1)
                .ok_or_else(|| Error::internal(format!("integer overflow for key {b:?}")))?;
            tx.write(coll, a, &write_int(next_a))?;
            tx.write(coll, b, &write_int(next_b))
        })
        .await?;
    }
    Ok(())
}

async fn read_only(db: &Database, coll: &Collection, keys: &[&[u8]]) -> Result<(), Error> {
    let values = db
        .tx(|tx| async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(tx.read(coll, key).await?);
            }
            Ok(values)
        })
        .await?;
    for (key, value) in keys.iter().zip(values) {
        if let Some(value) = value {
            assert_eq!(
                value.len(),
                std::mem::size_of::<i64>(),
                "key {key:?} has non-integer value {value:?}"
            );
            assert!(read_int(&value) >= 0, "negative value for {key:?}");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_workload(a1: u32, a2: u32, a3: u32, b1: u32, b2: u32, b3: u32) {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let db1 = Database::open("example", backend.clone()).await.unwrap();
    let db2 = Database::open("example", backend).await.unwrap();

    let (k1, k2, k3): (&[u8], &[u8], &[u8]) = (b"k1", b"k2", b"k3");

    let path = CollectionPath::new(b"fuzz-coll").unwrap();
    let coll1 = db1.create_collection_if_absent(&path).await.unwrap();
    let coll2 = db2.open_collection(&path).await.unwrap();
    let seed_coll = &coll1;
    db1.tx(|tx| async move {
        for k in [k1, k2, k3] {
            tx.write(seed_coll, k, &write_int(0))?;
        }
        Ok(())
    })
    .await
    .unwrap();

    // Database1 workload: rmw(k1), multi(k1,k2), read-only, rmw(k3).
    let w1 = async {
        rmw(&db1, &coll1, k1, a1).await?;
        multi_rmw(&db1, &coll1, k1, k2, a2).await?;
        read_only(&db1, &coll1, &[k1, k2, k3]).await?;
        rmw(&db1, &coll1, k3, a3).await?;
        Ok::<(), Error>(())
    };
    // Database2 workload: rmw(k2), multi(k2,k3), read-only, rmw(k1).
    let w2 = async {
        rmw(&db2, &coll2, k2, b1).await?;
        multi_rmw(&db2, &coll2, k2, k3, b2).await?;
        read_only(&db2, &coll2, &[k1, k2, k3]).await?;
        rmw(&db2, &coll2, k1, b3).await?;
        Ok::<(), Error>(())
    };

    let (r1, r2) = tokio::join!(w1, w2);
    r1.unwrap();
    r2.unwrap();

    // Each key's final value must equal the total increments applied to it.
    let v1 = read_int(&coll1.read(k1).await.unwrap().unwrap());
    let v2 = read_int(&coll1.read(k2).await.unwrap().unwrap());
    let v3 = read_int(&coll1.read(k3).await.unwrap().unwrap());
    assert_eq!(v1 as u32, a1 + a2 + b3, "k1 mismatch");
    assert_eq!(v2 as u32, a2 + b1 + b2, "k2 mismatch");
    assert_eq!(v3 as u32, a3 + b2, "k3 mismatch");

    db1.shutdown().await;
    db2.shutdown().await;
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn concurrent_tx_invariant(
        a1 in 0u32..4,
        a2 in 0u32..4,
        a3 in 0u32..4,
        b1 in 0u32..4,
        b2 in 0u32..4,
        b3 in 0u32..4,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        rt.block_on(run_workload(a1, a2, a3, b1, b2, b3));
    }
}
