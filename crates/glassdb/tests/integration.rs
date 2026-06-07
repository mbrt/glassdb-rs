//! Integration tests ported from the Go `glassdb_test.go` (memory-backend
//! subset). Time-sensitive paths use `tokio::time::pause` for determinism.

use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::{Backend, Collection, Ctx, Error, Tx, DB};

async fn init_db(b: Arc<dyn Backend>) -> DB {
    DB::open(&Ctx::background(), "example", b).await.unwrap()
}

fn mem() -> Arc<dyn Backend> {
    Arc::new(MemoryBackend::new())
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

async fn read_int_from_tx(tx: &Tx, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(v) => Ok(read_int(&v)),
        // Treat a missing value as zero (i.e. initialize it).
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

async fn rmw(ctx: &Ctx, db: &DB, coll: &Collection, key: &[u8], iters: usize) -> Result<(), Error> {
    for _ in 0..iters {
        db.tx(ctx, |tx| async move {
            let num = read_int_from_tx(&tx, coll, key).await?;
            tx.write(coll, key, &write_int(num + 1))
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn rw() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();

    coll.write(&ctx, key, val).await.unwrap();
    let buf = coll.read_strong(&ctx, key).await.unwrap();
    assert_eq!(buf, val);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 2);
    assert_eq!(stats.tx_writes, 1);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn delete() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();

    coll.write(&ctx, key, val).await.unwrap();
    coll.delete(&ctx, key).await.unwrap();

    let err = coll.read_strong(&ctx, key).await.unwrap_err();
    assert!(err.is_not_found(), "expected not-found, got {err:?}");

    let stats = db.stats();
    assert_eq!(stats.tx_n, 3);
    assert_eq!(stats.tx_writes, 2);
    assert!(stats.tx_retries <= 1);
}

#[tokio::test(start_paused = true)]
async fn read_from_another() {
    let ctx = Ctx::background();
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-another";
    let key = b"key1";
    let val = b"value1";

    db1.collection(coll).create(&ctx).await.unwrap();
    db1.collection(coll).write(&ctx, key, val).await.unwrap();

    let buf = db2.collection(coll).read_strong(&ctx, key).await.unwrap();
    assert_eq!(buf, val);
}

#[tokio::test(start_paused = true)]
async fn read_deleted_from_another() {
    let ctx = Ctx::background();
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-delete-another";
    let key1 = b"key1";
    let key2 = b"key2";
    let val = b"value1";
    let newval = b"value1-modified";

    let db1coll = db1.collection(coll);
    db1coll.create(&ctx).await.unwrap();
    let db1coll = &db1coll;
    db1.tx(&ctx, |tx| async move {
        tx.write(db1coll, key1, val)?;
        tx.write(db1coll, key2, val)
    })
    .await
    .unwrap();

    let db2coll = &db2.collection(coll);
    db2.tx(&ctx, |tx| async move {
        tx.write(db2coll, key1, newval)?;
        tx.delete(db2coll, key2)
    })
    .await
    .unwrap();

    let (key1_read, key2_found) = db1
        .tx(&ctx, |tx| async move {
            let k1 = tx.read(db1coll, key1).await?;
            let found = match tx.read(db1coll, key2).await {
                Ok(_) => true,
                Err(e) if e.is_not_found() => false,
                Err(e) => return Err(e),
            };
            Ok((k1, found))
        })
        .await
        .unwrap();

    assert_eq!(key1_read, newval);
    assert!(!key2_found);
}

#[tokio::test(start_paused = true)]
async fn rmw_single() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"rmw-c");
    let key = b"key";

    coll.create(&ctx).await.unwrap();
    rmw(&ctx, &db, &coll, key, 30).await.unwrap();

    let stats = db.stats();
    assert_eq!(stats.tx_n, 30);
    assert_eq!(stats.tx_reads, 30);
    assert_eq!(stats.tx_writes, 30);
    assert_eq!(stats.tx_retries, 0);

    let val = coll.read_strong(&ctx, key).await.unwrap();
    assert_eq!(read_int(&val), 30);
}

async fn multiple_rmw(
    ctx: &Ctx,
    db: &DB,
    coll: &Collection,
    key1: &[u8],
    key2: &[u8],
    iters: usize,
) -> Result<(), Error> {
    for _ in 0..iters {
        db.tx(ctx, |tx| async move {
            let n1 = read_int_from_tx(&tx, coll, key1).await?;
            tx.write(coll, key1, &write_int(n1 + 1))?;
            let n2 = read_int_from_tx(&tx, coll, key2).await?;
            tx.write(coll, key2, &write_int(n2 + 1))
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn concurrent_rmw() {
    let ctx = Ctx::background();
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key = b"key";

    db1.collection(coll_name).create(&ctx).await.unwrap();

    let coll1 = db1.collection(coll_name);
    let coll2 = db2.collection(coll_name);
    let (r1, r2) = tokio::join!(
        rmw(&ctx, &db1, &coll1, key, 30),
        rmw(&ctx, &db2, &coll2, key, 30),
    );
    r1.unwrap();
    r2.unwrap();

    let val = db2
        .collection(coll_name)
        .read_strong(&ctx, key)
        .await
        .unwrap();
    assert_eq!(read_int(&val), 60);
}

#[tokio::test(start_paused = true)]
async fn multiple_rmw_single() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"multiple-rmw-c");
    let key1 = b"key1";
    let key2 = b"key2";

    coll.create(&ctx).await.unwrap();
    multiple_rmw(&ctx, &db, &coll, key1, key2, 30)
        .await
        .unwrap();

    let val = coll.read_strong(&ctx, key1).await.unwrap();
    assert_eq!(read_int(&val), 30);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 31);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_multiple_rmw() {
    let ctx = Ctx::background();
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key1 = b"key1";
    let key2 = b"key2";

    db1.collection(coll_name).create(&ctx).await.unwrap();

    let coll1 = db1.collection(coll_name);
    let coll2 = db2.collection(coll_name);
    let (r1, r2) = tokio::join!(
        multiple_rmw(&ctx, &db1, &coll1, key1, key2, 30),
        multiple_rmw(&ctx, &db2, &coll2, key1, key2, 30),
    );
    r1.unwrap();
    r2.unwrap();

    let val = db2
        .collection(coll_name)
        .read_strong(&ctx, key1)
        .await
        .unwrap();
    assert_eq!(read_int(&val), 60);
    let val = db2
        .collection(coll_name)
        .read_strong(&ctx, key2)
        .await
        .unwrap();
    assert_eq!(read_int(&val), 60);
}

// Builds the 16-byte priority-encoded transaction id used to pin a
// transaction's wound-wait priority: `[8-byte name prefix][8-byte BE unix-nanos]`.
// A smaller `secs` is older (higher priority).
fn mk_tid_bytes(secs: u64, name: &[u8]) -> Vec<u8> {
    let mut b = vec![0u8; 16];
    let n = name.len().min(8);
    b[..n].copy_from_slice(&name[..n]);
    b[8..].copy_from_slice(&(secs * 1_000_000_000).to_be_bytes());
    b
}

async fn wound_incr(
    db: &DB,
    ctx: &Ctx,
    coll: &Collection,
    first: &[u8],
    second: &[u8],
) -> Result<(), Error> {
    db.tx(ctx, |tx| async move {
        let a = read_int_from_tx(&tx, coll, first).await?;
        let b = read_int_from_tx(&tx, coll, second).await?;
        // Under paused time the clock only advances once every task is blocked,
        // so sleeping here parks both transactions after their reads and before
        // any write: they both observe the initial values and then commit
        // concurrently, forcing the conflict the rule must resolve.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        tx.write(coll, first, &write_int(a + 1))?;
        tx.write(coll, second, &write_int(b + 1))
    })
    .await
}

// Exercises the classic deadlock setup: two concurrent transactions touch the
// same two keys in opposite orders. Under the wound-wait rule the older
// transaction wins and the younger one restarts, rather than both stalling
// until the deadlock-timeout fallback fires.
#[tokio::test(start_paused = true)]
async fn wound_wait_reverse_contention() {
    use std::time::Duration;

    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"wound-wait-c");
    let key1 = b"key1";
    let key2 = b"key2";
    coll.create(&ctx).await.unwrap();

    // Deterministic priorities so the conflict has a well-defined winner: the
    // "older" transaction has higher priority than the "younger" one.
    let ctx_old = ctx.with_tx_id(mk_tid_bytes(1, b"older"));
    let ctx_young = ctx.with_tx_id(mk_tid_bytes(2, b"younger"));

    let start = tokio::time::Instant::now();
    let (r1, r2) = tokio::join!(
        wound_incr(&db, &ctx_old, &coll, key1, key2),
        wound_incr(&db, &ctx_young, &coll, key2, key1),
    );
    r1.unwrap();
    r2.unwrap();
    let elapsed = start.elapsed();

    // Both increments landed exactly once on top of each other.
    let v1 = coll.read_strong(&ctx, key1).await.unwrap();
    assert_eq!(read_int(&v1), 2);
    let v2 = coll.read_strong(&ctx, key2).await.unwrap();
    assert_eq!(read_int(&v2), 2);

    // The contention forced one transaction to restart...
    assert!(db.stats().tx_retries >= 1, "expected at least one retry");
    // ...but wound-wait resolved it promptly instead of waiting out the
    // multi-second deadlock-timeout fallback (MAX_DEADLOCK_TIMEOUT = 5s).
    assert!(
        elapsed < Duration::from_secs(5),
        "took too long: {elapsed:?}"
    );
}

// Reads many keys concurrently within a single transaction (the parallelism
// `read_multi` used to provide), now via `join_all` over `Tx::read`.
#[tokio::test(start_paused = true)]
async fn concurrent_reads() {
    use futures::future::join_all;

    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = &db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();

    let keys: Vec<Vec<u8>> = (0..15).map(|i| format!("key{i}").into_bytes()).collect();
    let keys = &keys;

    // Initialize the values.
    db.tx(&ctx, |tx| async move {
        for k in keys {
            tx.write(coll, k, &write_int(0))?;
        }
        Ok(())
    })
    .await
    .unwrap();

    // Read all (in parallel) and increment.
    for _ in 0..30 {
        db.tx(&ctx, |tx| async move {
            let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            for (k, r) in keys.iter().zip(vals) {
                let cur = read_int(&r?);
                tx.write(coll, k, &write_int(cur + 1))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }

    let stats = db.stats();
    assert_eq!(stats.tx_n, 31);
    assert_eq!(stats.tx_retries, 0);

    for k in keys {
        let b = coll.read_strong(&ctx, k).await.unwrap();
        assert_eq!(read_int(&b), 30);
    }
}

#[tokio::test(start_paused = true)]
async fn read_weak() {
    use std::time::Duration;

    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();
    let key = b"key";

    let staleness = Duration::from_millis(300);
    let sleep_time = Duration::from_millis(100);
    let max_behind = (staleness.as_millis() / sleep_time.as_millis()) as i64 + 1;

    let coll = &coll;
    for i in 0..30i64 {
        // Increment the value. The read avoids making this a blind write.
        db.tx(&ctx, |tx| async move {
            let _ = read_int_from_tx(&tx, coll, key).await?;
            tx.write(coll, key, &write_int(i))
        })
        .await
        .unwrap();

        let val = coll.read_weak(&ctx, key, staleness).await.unwrap();
        let read_num = read_int(&val);
        assert!(read_num <= i, "weak read {read_num} should be <= {i}");
        if i >= max_behind {
            assert!(read_num >= i - max_behind);
        }

        tokio::time::sleep(sleep_time).await;
    }

    let stats = db.stats();
    assert_eq!(stats.tx_n, 30);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn list_keys() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();

    let keys: Vec<Vec<u8>> = (0u32..100).map(|i| i.to_be_bytes().to_vec()).collect();
    let test_val = b"val";
    let coll_ref = &coll;
    let keys_ref = &keys;
    db.tx(&ctx, |tx| async move {
        for k in keys_ref {
            tx.write(coll_ref, k, test_val)?;
        }
        Ok(())
    })
    .await
    .unwrap();

    let mut iter = coll.keys(&ctx).await.unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for k in iter.by_ref() {
        got.push(k);
    }
    assert!(iter.err().is_none(), "iter error: {:?}", iter.err());

    assert_eq!(got.len(), keys.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for k in &keys {
        assert!(got_set.contains(k), "missing key {k:?}");
    }
    // The listing must be sorted.
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);

    let stats = db.stats();
    assert_eq!(stats.obj_lists, 1);
}

#[tokio::test(start_paused = true)]
async fn list_collections() {
    let ctx = Ctx::background();
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();

    let colls: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();
    for c in &colls {
        coll.collection(c).create(&ctx).await.unwrap();
    }

    let mut iter = coll.collections(&ctx).await.unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for c in iter.by_ref() {
        got.push(c);
    }
    assert!(iter.err().is_none(), "iter error: {:?}", iter.err());

    assert_eq!(got.len(), colls.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for c in &colls {
        assert!(got_set.contains(c), "missing collection {c:?}");
    }
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
}

#[tokio::test(start_paused = true)]
async fn builder_custom_options() {
    use std::time::Duration;

    let ctx = Ctx::background();
    let db = DB::builder("example", mem())
        .cache_size(8 * 1024 * 1024)
        .retry_initial_interval(Duration::from_millis(10))
        .retry_max_interval(Duration::from_millis(100))
        .open(&ctx)
        .await
        .unwrap();

    let coll = db.collection(b"demo-coll");
    coll.create(&ctx).await.unwrap();
    coll.write(&ctx, b"key1", b"value1").await.unwrap();
    let buf = coll.read_strong(&ctx, b"key1").await.unwrap();
    assert_eq!(buf, b"value1");
}
