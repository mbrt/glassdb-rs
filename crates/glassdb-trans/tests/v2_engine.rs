//! End-to-end tests for the minimal v2 engine (ADR-016 … ADR-021) on the
//! in-memory backend. These exercise the object-storage-native layout directly:
//! shards as the lock table + MVCC index, the collection root for membership,
//! and unified transaction objects, with strict-serializable wound-wait.

use std::sync::Arc;

use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_data::shard::shard_index;
use glassdb_storage::StorageError;
use glassdb_trans::TransError;
use glassdb_trans::v2::Collection;

fn backend() -> Arc<dyn Backend> {
    Arc::new(MemoryBackend::new())
}

async fn new_collection() -> Collection {
    Collection::create(backend(), "db/c").await.unwrap()
}

fn u64_bytes(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn parse_u64(v: Option<Vec<u8>>) -> u64 {
    v.map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .unwrap_or(0)
}

#[tokio::test]
async fn put_get_roundtrip() {
    let c = new_collection().await;
    assert_eq!(c.get(b"k").await.unwrap(), None);
    c.put(b"k", b"v".to_vec()).await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn overwrite_existing_key() {
    let c = new_collection().await;
    c.put(b"k", b"v1".to_vec()).await.unwrap();
    c.put(b"k", b"v2".to_vec()).await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), Some(b"v2".to_vec()));
}

#[tokio::test]
async fn delete_makes_key_absent() {
    let c = new_collection().await;
    c.put(b"k", b"v".to_vec()).await.unwrap();
    c.delete(b"k").await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), None);
    // Deleting an absent key is a no-op.
    c.delete(b"missing").await.unwrap();
}

#[tokio::test]
async fn open_missing_collection_errors() {
    let err = Collection::open(backend(), "db/none").await.unwrap_err();
    // A missing collection surfaces as the root object being not found.
    assert!(matches!(err, TransError::Storage(StorageError::NotFound)));
}

#[tokio::test]
async fn multi_key_transaction_is_atomic() {
    let c = new_collection().await;
    c.transact(|tx| async move {
        tx.put(b"a", b"1".to_vec());
        tx.put(b"b", b"2".to_vec());
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(c.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(c.get(b"b").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn read_your_writes_within_transaction() {
    let c = new_collection().await;
    let seen = c
        .transact(|tx| async move {
            tx.put(b"k", b"staged".to_vec());
            tx.get(b"k").await
        })
        .await
        .unwrap();
    assert_eq!(seen, Some(b"staged".to_vec()));
}

#[tokio::test]
async fn list_reflects_committed_keys() {
    let c = new_collection().await;
    assert!(c.list().await.unwrap().is_empty());
    c.put(b"a", b"1".to_vec()).await.unwrap();
    c.put(b"b", b"2".to_vec()).await.unwrap();
    c.put(b"c", b"3".to_vec()).await.unwrap();
    let mut keys = c.list().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    c.delete(b"b").await.unwrap();
    let mut keys = c.list().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn many_keys_roundtrip_across_shards() {
    let c = new_collection().await;
    for i in 0..50u64 {
        c.put(format!("key-{i}").as_bytes(), u64_bytes(i))
            .await
            .unwrap();
    }
    for i in 0..50u64 {
        assert_eq!(
            parse_u64(c.get(format!("key-{i}").as_bytes()).await.unwrap()),
            i
        );
    }
    assert_eq!(c.list().await.unwrap().len(), 50);
}

// Two distinct keys that hash to the *same* shard must both commit when written
// concurrently: shard-CAS contention is merge-on-retry, not a lock conflict
// (ADR-020).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_keys_same_shard_both_commit() {
    let c = new_collection().await;
    let base = b"k0";
    let target = shard_index(base);
    // Find a second key in the same shard.
    let mut other = None;
    for i in 1..100_000u64 {
        let cand = format!("k{i}");
        if shard_index(cand.as_bytes()) == target {
            other = Some(cand);
            break;
        }
    }
    let other = other.expect("a colliding key exists");
    assert_eq!(shard_index(other.as_bytes()), target);

    let c1 = c.clone();
    let c2 = c.clone();
    let other2 = other.clone();
    let h1 = tokio::spawn(async move { c1.put(b"k0", b"v0".to_vec()).await });
    let h2 = tokio::spawn(async move { c2.put(other2.as_bytes(), b"v1".to_vec()).await });
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();

    assert_eq!(c.get(b"k0").await.unwrap(), Some(b"v0".to_vec()));
    assert_eq!(c.get(other.as_bytes()).await.unwrap(), Some(b"v1".to_vec()));
}

// The serializability stress test: many concurrent read-modify-write increments
// of one counter must total exactly the number of increments (no lost updates).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_counter_increments_are_serializable() {
    let c = new_collection().await;
    c.put(b"counter", u64_bytes(0)).await.unwrap();

    const N: u64 = 16;
    let mut handles = Vec::new();
    for _ in 0..N {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            c.transact(|tx| async move {
                let cur = parse_u64(tx.get(b"counter").await?);
                tx.put(b"counter", u64_bytes(cur + 1));
                Ok(())
            })
            .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert_eq!(parse_u64(c.get(b"counter").await.unwrap()), N);
}

// Concurrent blind writers to the same key all commit (wound-wait guarantees
// progress); the final value is one of the writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_same_key_make_progress() {
    let c = new_collection().await;
    c.put(b"k", u64_bytes(0)).await.unwrap();

    const N: u64 = 8;
    let mut handles = Vec::new();
    for i in 1..=N {
        let c = c.clone();
        handles.push(tokio::spawn(async move { c.put(b"k", u64_bytes(i)).await }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let final_val = parse_u64(c.get(b"k").await.unwrap());
    assert!(
        (1..=N).contains(&final_val),
        "unexpected final value {final_val}"
    );
}

// A transaction spanning two keys in different shards observes a consistent
// snapshot: a concurrent transfer between them never lets the reader see money
// created or destroyed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_transfer_preserves_invariant() {
    let c = new_collection().await;
    c.put(b"acct-a", u64_bytes(100)).await.unwrap();
    c.put(b"acct-b", u64_bytes(0)).await.unwrap();

    const ROUNDS: u64 = 20;
    let transferer = {
        let c = c.clone();
        tokio::spawn(async move {
            for _ in 0..ROUNDS {
                c.transact(|tx| async move {
                    let a = parse_u64(tx.get(b"acct-a").await?);
                    let b = parse_u64(tx.get(b"acct-b").await?);
                    if a > 0 {
                        tx.put(b"acct-a", u64_bytes(a - 1));
                        tx.put(b"acct-b", u64_bytes(b + 1));
                    }
                    Ok(())
                })
                .await
                .unwrap();
            }
        })
    };

    // Concurrent auditor: the total must always be exactly 100.
    let auditor = {
        let c = c.clone();
        tokio::spawn(async move {
            for _ in 0..ROUNDS {
                let total = c
                    .transact(|tx| async move {
                        let a = parse_u64(tx.get(b"acct-a").await?);
                        let b = parse_u64(tx.get(b"acct-b").await?);
                        Ok(a + b)
                    })
                    .await
                    .unwrap();
                assert_eq!(total, 100, "invariant violated: total={total}");
            }
        })
    };

    transferer.await.unwrap();
    auditor.await.unwrap();
    assert_eq!(parse_u64(c.get(b"acct-a").await.unwrap()), 80);
    assert_eq!(parse_u64(c.get(b"acct-b").await.unwrap()), 20);
}
