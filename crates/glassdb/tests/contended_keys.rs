//! Leaves that separate the keys that several database instances write
//! (ADR-085).

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use glassdb::middleware::HookBackend;
use glassdb::{Backend, Collection, Database, KeyScan, RestructurerStats};

pub mod integration_support;

use integration_support::{create_top, mem, open_top, write_int};

const BACKEND_DELAY: Duration = Duration::from_millis(5);

// Under paused time, the operations of the memory backend take no time, so
// two database instances would never send leaf CASes at the same time. Each
// operation of this backend takes time.
fn slow_mem() -> Arc<dyn Backend> {
    let backend = HookBackend::new(mem());
    backend.set_before(|_| {
        Box::pin(async {
            tokio::time::sleep(BACKEND_DELAY).await;
            Ok(())
        })
    });
    backend
}

// Four database instances over one collection of 8 keys, which the first one
// wrote. The leaf of the 8 keys is below the soft cap.
async fn four_instances() -> (Vec<Database>, Vec<Collection>) {
    let backend = slow_mem();
    let mut dbs = Vec::new();
    for _ in 0..4 {
        dbs.push(Database::open("example", backend.clone()).await.unwrap());
    }
    let coll = create_top(&dbs[0], b"contended").await;
    for key in 0..8 {
        coll.write(&[key], &write_int(0)).await.unwrap();
    }
    let mut colls = vec![coll];
    for db in &dbs[1..] {
        colls.push(open_top(db, b"contended").await);
    }
    (dbs, colls)
}

// Writes each key of `keys` `rounds` times, each key from its own task.
async fn write_concurrently(coll: &Collection, keys: impl IntoIterator<Item = u8>, rounds: i64) {
    let writers: Vec<_> = keys
        .into_iter()
        .map(|key| {
            let coll = coll.clone();
            tokio::spawn(async move {
                for round in 0..rounds {
                    coll.write(&[key], &write_int(round)).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
}

// Each database instance writes its own key of the keys 0 to 3, so the
// database instances lose leaf CASes to each other.
async fn write_one_key_each(colls: &[Collection]) {
    join_all(
        colls
            .iter()
            .zip(0u8..)
            .map(|(coll, key)| write_concurrently(coll, [key], 1000)),
    )
    .await;
}

fn sum(dbs: &[Database]) -> RestructurerStats {
    let mut stats = RestructurerStats::default();
    for db in dbs {
        stats += db.stats().restructurer;
    }
    stats
}

async fn shutdown(dbs: Vec<Database>) {
    for db in dbs {
        db.shutdown().await;
    }
}

// The leaf splits to separate the keys of the database instances, and the
// leaves below the underfull threshold do not merge back while the database
// instances keep writing.
#[tokio::test(start_paused = true)]
async fn the_keys_of_database_instances_split_apart() {
    let (dbs, colls) = four_instances().await;

    write_one_key_each(&colls).await;

    let stats = sum(&dbs);
    assert!(stats.contention_splits >= 1, "{stats:?}");
    assert_eq!(stats.merges, 0, "{stats:?}");
    assert!(stats.kept_apart >= 1, "{stats:?}");
    let keys = colls[0].scan_keys(KeyScan::all()).await.unwrap();
    assert_eq!(keys.keys().len(), 8);
    shutdown(dbs).await;
}

// The rounds of one database instance share a leaf CAS, so its writers lose
// no leaf CASes to each other, and its keys stay in one leaf.
#[tokio::test(start_paused = true)]
async fn the_keys_that_one_database_instance_writes_stay_together() {
    let (dbs, colls) = four_instances().await;

    write_concurrently(&colls[0], 0..8, 1000).await;

    let stats = sum(&dbs);
    assert_eq!((stats.splits, stats.contention_splits), (0, 0), "{stats:?}");
    shutdown(dbs).await;
}

// After the database instances stop, their writers stop being recent. Then the
// writes of one database instance to all keys merge the leaves again.
#[tokio::test(start_paused = true)]
async fn the_leaves_merge_again_when_their_writers_stop() {
    let (dbs, colls) = four_instances().await;
    write_one_key_each(&colls).await;
    let split = sum(&dbs);
    assert!(split.contention_splits >= 1, "{split:?}");

    tokio::time::sleep(Duration::from_secs(61)).await;
    for _ in 0..3 {
        write_concurrently(&colls[0], 0..8, 1).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let merged = sum(&dbs);
    assert!(merged.merges >= 1, "{merged:?}");
    shutdown(dbs).await;
}
