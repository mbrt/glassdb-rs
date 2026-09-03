//! Basic database and transaction integration behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use glassdb::{Database, Error, InlinePolicy, ProtocolTiming, SplitPolicy};
use glassdb_data::TxId;
use glassdb_storage::{CurrentState, LeafBody, LeafEntry, Node};

#[path = "integration_support/mod.rs"]
pub mod integration_support;

use integration_support::{
    create_top, incremented_value, init_db, mem, multiple_rmw, open_top, read_int,
    read_int_from_tx, rmw, try_read_int, write_int,
};

fn split_unsafe_boundary_key(policy: &SplitPolicy, value: &[u8], fill: u8) -> Vec<u8> {
    let writer = TxId::with_priority(1, b"boundary");
    let mut boundary = None;
    for len in 1..policy.content_limit() {
        let key = vec![fill; len];
        let inline = LeafEntry::new(key.clone()).with_current(CurrentState::Inline {
            writer: writer.clone(),
            value: Arc::from(value),
        });
        if policy.key_fits(&key) && !policy.entry_fits_split_budget(&inline) {
            boundary = Some(key);
        }
    }
    boundary.expect("test policy has no accepted key whose inline form exceeds its split budget")
}

#[tokio::test(start_paused = true)]
async fn rw() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    coll.write(key, val).await.unwrap();
    let buf = coll.read(key).await.unwrap().unwrap();
    assert_eq!(buf, val);

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 3);
    assert_eq!(stats.transactions.writes, 1);
    assert_eq!(stats.transactions.retries, 0);
}

#[tokio::test]
async fn individually_oversized_key_is_invalid_input() {
    let policy = SplitPolicy::builder()
        .node_max_bytes(256)
        .split_headroom_bytes(64)
        .build()
        .unwrap();
    let db = Database::builder("example", mem())
        .split_policy(policy)
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo")
        .await
        .unwrap();
    let before = db.stats();

    let err = coll.write(&[b'k'; 128], b"value").await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
    let delta = db.stats() - before;
    assert_eq!(
        delta.locker.calls, 0,
        "invalid keys are rejected before locking"
    );
}

#[tokio::test(start_paused = true)]
async fn boundary_inline_falls_back_before_it_can_strand_a_leaf() {
    let policy = SplitPolicy::builder()
        .node_soft_max_bytes(384)
        .node_max_bytes(512)
        .split_headroom_bytes(128)
        .build()
        .unwrap();
    let inline_value = vec![b'v'; 128];
    let first = split_unsafe_boundary_key(&policy, &inline_value, b'a');
    let second = vec![b'z'; first.len()];
    assert!(policy.key_fits(&second));
    let boundary_writer = TxId::with_priority(1, b"boundary");
    let inline_entry = LeafEntry::new(first.clone()).with_current(CurrentState::Inline {
        writer: boundary_writer,
        value: Arc::from(inline_value.as_slice()),
    });
    let mut create_entry = LeafEntry::new(second.clone());
    create_entry.replace_create_lock(TxId::with_priority(2, b"creator"));
    assert!(
        Node::leaf(LeafBody::from_entries([inline_entry, create_entry])).content_encoded_len()
            > policy.content_limit(),
        "the old inline publication must make the second accepted key hit LeafFull"
    );

    let db = Database::builder("example", mem())
        .split_policy(policy)
        .inline_policy(InlinePolicy {
            max_value_bytes: inline_value.len(),
            max_leaf_bytes: inline_value.len(),
        })
        .open()
        .await
        .unwrap();
    let coll = create_top(&db, b"boundary").await;
    coll.write(&first, b"seed").await.unwrap();

    let before = db.stats();
    let coll_ref = &coll;
    let first_ref = first.as_slice();
    let inline_ref = inline_value.as_slice();
    db.tx(|tx| async move {
        tx.read(coll_ref, first_ref).await?.ok_or(Error::NotFound)?;
        tx.write(coll_ref, first_ref, inline_ref)
    })
    .await
    .unwrap();
    let direct = (db.stats() - before).direct_commit;
    assert_eq!(direct.candidates, 1);
    assert_eq!(direct.landed, 0, "the split-unsafe inline value fell back");

    tokio::time::timeout(Duration::from_secs(5), coll.write(&second, b"second"))
        .await
        .expect("a second accepted key must not wait forever for an impossible split")
        .unwrap();
    assert_eq!(coll.read(&first).await.unwrap().unwrap(), inline_value);
    assert_eq!(coll.read(&second).await.unwrap().unwrap(), b"second");
}
#[tokio::test(start_paused = true)]
async fn delete() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    coll.write(key, val).await.unwrap();
    coll.delete(key).await.unwrap();

    assert!(coll.read(key).await.unwrap().is_none());
    assert!(
        coll.read_stale(key, std::time::Duration::MAX)
            .await
            .unwrap()
            .is_none()
    );

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 4);
    assert_eq!(stats.transactions.writes, 2);
    assert!(stats.transactions.retries <= 1);
}

/// Regression: reading a found key and deleting that same key in one
/// transaction must commit. ADR-061 publishes the deletion as an authoritative
/// tombstone in the direct leaf CAS; before direct deletes were supported this
/// shape had to route cleanly through the locked protocol rather than reaching
/// a conditional-delete path without a lock.
#[tokio::test(start_paused = true)]
async fn read_then_delete_single_tx() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    coll.write(key, val).await.unwrap();

    // Read the existing value, then delete the same key, in one transaction.
    let coll = &coll;
    let prev = db
        .tx(|tx| async move {
            let v = tx.read(coll, key).await?.ok_or(Error::NotFound)?;
            tx.delete(coll, key)?;
            Ok(v)
        })
        .await
        .expect("a single read-then-delete transaction must commit");
    assert_eq!(prev, val);

    assert!(coll.read(key).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn read_from_another() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-another";
    let key = b"key1";
    let val = b"value1";

    let db1coll = create_top(&db1, coll).await;
    db1coll.write(key, val).await.unwrap();

    let buf = open_top(&db2, coll).await.read(key).await.unwrap().unwrap();
    assert_eq!(buf, val);
}

#[tokio::test(start_paused = true)]
async fn read_deleted_from_another() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-delete-another";
    let key1 = b"key1";
    let key2 = b"key2";
    let val = b"value1";
    let newval = b"value1-modified";

    let db1coll = db1
        .root_collection()
        .create_collection_if_absent(coll)
        .await
        .unwrap();
    let db1coll = &db1coll;
    db1.tx(|tx| async move {
        tx.write(db1coll, key1, val)?;
        tx.write(db1coll, key2, val)
    })
    .await
    .unwrap();

    let db2coll = &open_top(&db2, coll).await;
    db2.tx(|tx| async move {
        tx.write(db2coll, key1, newval)?;
        tx.delete(db2coll, key2)
    })
    .await
    .unwrap();

    let (key1_read, key2_found) = db1
        .tx(|tx| async move {
            let k1 = tx.read(db1coll, key1).await?.ok_or(Error::NotFound)?;
            let found = tx.read(db1coll, key2).await?.is_some();
            Ok((k1, found))
        })
        .await
        .unwrap();

    assert_eq!(key1_read, newval);
    assert!(!key2_found);
}

#[tokio::test(start_paused = true)]
async fn rmw_single() {
    let db = init_db(mem()).await;
    let key = b"key";

    let coll = create_top(&db, b"rmw-c").await;
    rmw(&db, &coll, key, 30).await.unwrap();

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 31);
    assert_eq!(stats.transactions.reads, 30);
    assert_eq!(stats.transactions.writes, 30);
    assert_eq!(stats.transactions.retries, 0);

    let val = coll.read(key).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 30);
}

#[tokio::test(start_paused = true)]
async fn concurrent_rmw() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key = b"key";

    let coll1 = create_top(&db1, coll_name).await;
    let coll2 = open_top(&db2, coll_name).await;
    let (r1, r2) = tokio::join!(rmw(&db1, &coll1, key, 30), rmw(&db2, &coll2, key, 30),);
    r1.unwrap();
    r2.unwrap();

    let val = coll2.read(key).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
}

// ADR-053: a single-key read-modify-write whose version is superseded before it
// publishes replays its body, reevaluating against the winner under the same
// transaction. The caller sees one successful commit applied exactly once, and
// the key stays on the logless path — falling back to locking would publish a
// holder that pushes its next writer off the direct path for no reason.
#[tokio::test(start_paused = true)]
async fn a_superseded_read_modify_write_replays_without_locking() {
    let db = init_db(mem()).await;
    let coll = create_top(&db, b"replay-c").await;
    coll.write(b"key", &write_int(1)).await.unwrap();

    let before = db.stats();
    let attempts = AtomicUsize::new(0);
    db.tx(|tx| {
        let coll = coll.clone();
        let attempts = &attempts;
        async move {
            let n = read_int_from_tx(&tx, &coll, b"key").await?;
            // Supersede the read exactly once, after the body observed it.
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                coll.write(b"key", &write_int(100)).await?;
            }
            tx.write(&coll, b"key", &incremented_value(b"key", n, 1)?)
        }
    })
    .await
    .unwrap();

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "the superseded attempt replays its body once"
    );
    assert_eq!(
        read_int(&coll.read(b"key").await.unwrap().unwrap()),
        101,
        "the replay incremented the winner's value, not the stale one it read"
    );

    // The interposed overwrite and the replayed commit both land directly, and
    // neither the loss nor the replay ever acquires a lock.
    let delta = db.stats() - before;
    assert_eq!(delta.direct_commit.landed, 2);
    assert_eq!(delta.locker.calls, 0, "a replayed loss publishes no holder");
    assert_eq!(delta.transactions.retries, 1);
}

#[tokio::test(start_paused = true)]
async fn multiple_rmw_single() {
    let db = init_db(mem()).await;
    let key1 = b"key1";
    let key2 = b"key2";

    let coll = create_top(&db, b"multiple-rmw-c").await;
    multiple_rmw(&db, &coll, key1, key2, 30).await.unwrap();

    let val = coll.read(key1).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 30);

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 32);
    assert_eq!(stats.transactions.retries, 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_multiple_rmw() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key1 = b"key1";
    let key2 = b"key2";

    let coll1 = create_top(&db1, coll_name).await;
    let coll2 = open_top(&db2, coll_name).await;
    let (r1, r2) = tokio::join!(
        multiple_rmw(&db1, &coll1, key1, key2, 30),
        multiple_rmw(&db2, &coll2, key1, key2, 30),
    );
    r1.unwrap();
    r2.unwrap();

    let val = coll2.read(key1).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
    let val = coll2.read(key2).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
}

// Reads many keys concurrently within a single transaction (the parallelism
// `read_multi` used to provide), now via `join_all` over `Transaction::read`.
#[tokio::test(start_paused = true)]
async fn concurrent_reads() {
    use futures::future::join_all;

    let db = init_db(mem()).await;
    let coll = &create_top(&db, b"demo-coll").await;

    let keys: Vec<Vec<u8>> = (0..15).map(|i| format!("key{i}").into_bytes()).collect();
    let keys = &keys;

    // Initialize the values.
    db.tx(|tx| async move {
        for k in keys {
            tx.write(coll, k, &write_int(0))?;
        }
        Ok(())
    })
    .await
    .unwrap();

    // Read all (in parallel) and increment.
    for _ in 0..30 {
        db.tx(|tx| async move {
            let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            for (k, r) in keys.iter().zip(vals) {
                let value = r?.ok_or(Error::NotFound)?;
                let cur = try_read_int(&value).ok_or_else(|| {
                    Error::internal(format!("key {k:?} has invalid integer value {value:?}"))
                })?;
                tx.write(coll, k, &incremented_value(k, cur, 1)?)?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 32);
    assert_eq!(stats.transactions.retries, 0);

    for k in keys {
        let b = coll.read(k).await.unwrap().unwrap();
        assert_eq!(read_int(&b), 30);
    }
}

#[tokio::test(start_paused = true)]
async fn read_stale() {
    use std::time::Duration;

    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    let key = b"key";

    let staleness = Duration::from_millis(300);
    let sleep_time = Duration::from_millis(100);
    let max_behind = (staleness.as_millis() / sleep_time.as_millis()) as i64 + 1;

    let coll = &coll;
    for i in 0..30i64 {
        // Increment the value. The read avoids making this a blind write.
        db.tx(|tx| async move {
            let _ = read_int_from_tx(&tx, coll, key).await?;
            tx.write(coll, key, &write_int(i))
        })
        .await
        .unwrap();

        let val = coll.read_stale(key, staleness).await.unwrap().unwrap();
        let read_num = read_int(&val);
        assert!(read_num <= i, "weak read {read_num} should be <= {i}");
        if i >= max_behind {
            assert!(read_num >= i - max_behind);
        }

        tokio::time::sleep(sleep_time).await;
    }

    let stats = db.stats();
    assert_eq!(stats.transactions.completed, 31);
    assert_eq!(stats.transactions.retries, 0);
}
#[tokio::test(start_paused = true)]
async fn builder_custom_options() {
    use std::time::Duration;

    let db = Database::builder("example", mem())
        .cache_size(8 * 1024 * 1024)
        .retry_initial_interval(Duration::from_millis(10))
        .retry_max_interval(Duration::from_millis(100))
        .protocol_timing(ProtocolTiming::new(
            Duration::from_secs(1),
            Duration::from_secs(2),
        ))
        .open()
        .await
        .unwrap();

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    coll.write(b"key1", b"value1").await.unwrap();
    let buf = coll.read(b"key1").await.unwrap().unwrap();
    assert_eq!(buf, b"value1");
}
