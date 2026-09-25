//! Statistics and diagnostics integration behavior.

use glassdb::{Database, Error, InlinePolicy};

pub mod integration_support;

use integration_support::{create_top, init_db, mem, open_top, read_int, rmw, write_int};

// The distributed locker's counters are surfaced through `Database::stats()`
// (the same reset-on-read accumulation pattern as the backend object counters),
// not only through the internal diagnostics snapshot. A committed write
// transaction uses locked commit (a read-only commit does not), so it
// must bump `locker.calls` while a pure read leaves the counter unchanged.
#[tokio::test(start_paused = true)]
async fn stats_report_locker_activity() {
    let db = Database::builder("example", mem())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let before = db.stats();
    coll.write(b"key1", b"value1").await.unwrap();
    let after_write = db.stats();
    assert!(
        after_write.locker.calls > before.locker.calls,
        "a committed write must report locker calls: {} -> {}",
        before.locker.calls,
        after_write.locker.calls
    );
    assert!(
        after_write.coordinator.submissions > before.coordinator.submissions,
        "a committed write must submit coordinator work: {} -> {}",
        before.coordinator.submissions,
        after_write.coordinator.submissions
    );
    assert!(
        after_write.coordinator.rounds > before.coordinator.rounds,
        "a committed write must start coordinator rounds: {} -> {}",
        before.coordinator.rounds,
        after_write.coordinator.rounds
    );
    assert!(
        after_write.coordinator.submissions >= after_write.coordinator.rounds,
        "one round cannot serve more work than was submitted"
    );

    // A read-only transaction commits via optimistic validation, so the
    // counter is unchanged across it.
    let _ = coll.read(b"key1").await.unwrap();
    let after_read = db.stats();
    assert_eq!(
        after_read.locker.calls, after_write.locker.calls,
        "a read-only commit takes no locks"
    );

    db.shutdown().await;
    let drained = db.stats();
    assert!(drained.coordinator.submissions >= after_write.coordinator.submissions);
    assert_eq!(db.stats(), drained, "stats snapshots remain cumulative");
}

#[tokio::test(start_paused = true)]
async fn stats_report_direct_commit_coverage() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"direct-stats")
        .await
        .unwrap();
    coll.write(b"key", b"before").await.unwrap();
    let before = db.stats();

    db.tx(|tx| {
        let coll = coll.clone();
        async move {
            let value = tx.read(&coll, b"key").await?.ok_or(Error::NotFound)?;
            tx.write(&coll, b"key", &value)
        }
    })
    .await
    .unwrap();

    let delta = db.stats() - before;
    assert_eq!(delta.direct_commit.candidates, 1);
    assert_eq!(delta.direct_commit.landed, 1);
}

#[tokio::test(start_paused = true)]
async fn aggregate_inline_pressure_splits_for_a_later_direct_commit() {
    let db = Database::builder("example", mem())
        .inline_policy(InlinePolicy {
            max_value_bytes: 8,
            max_leaf_bytes: 8,
        })
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"inline-pressure")
        .await
        .unwrap();
    coll.write(b"a", &write_int(0)).await.unwrap();
    coll.write(b"b", &write_int(0)).await.unwrap();

    let before_first = db.stats();
    rmw(&db, &coll, b"a", 1).await.unwrap();
    let first = db.stats() - before_first;
    assert_eq!(first.direct_commit.candidates, 1);
    assert_eq!(first.direct_commit.landed, 1);

    let before_miss = db.stats();
    rmw(&db, &coll, b"b", 1).await.unwrap();
    let miss = db.stats() - before_miss;
    assert_eq!(miss.direct_commit.candidates, 1);
    assert_eq!(miss.direct_commit.landed, 0);
    assert!(
        miss.locker.calls > 0,
        "the discovering mutation completes through the locked fallback"
    );

    let before_split = db.stats();
    let mut after_split = before_split;
    for _ in 0..5 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        after_split = db.stats();
        if after_split.restructurer.inline_pressure.completed
            > before_split.restructurer.inline_pressure.completed
        {
            break;
        }
    }
    let split = after_split - before_split;
    assert_eq!(split.restructurer.inline_pressure.candidates, 1);
    assert_eq!(split.restructurer.inline_pressure.completed, 1);
    assert_eq!(split.restructurer.inline_pressure.discarded, 0);

    let before_retry = db.stats();
    rmw(&db, &coll, b"b", 1).await.unwrap();
    let retry = db.stats() - before_retry;
    assert_eq!(retry.direct_commit.candidates, 1);
    assert_eq!(retry.direct_commit.landed, 1);
    assert_eq!(
        retry.locker.calls, 0,
        "the split gives the later mutation enough inline headroom"
    );
    assert_eq!(read_int(&coll.read(b"a").await.unwrap().unwrap()), 1);
    assert_eq!(read_int(&coll.read(b"b").await.unwrap().unwrap()), 2);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn stats_report_cache_owner_activity() {
    let backend = mem();
    let writer_db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let reader_db = Database::builder("example", backend)
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let key = b"key";
    let value = b"value";

    let writer = create_top(&writer_db, b"cache-stats").await;
    let reader = open_top(&reader_db, b"cache-stats").await;
    writer.write(key, value).await.unwrap();
    writer_db.shutdown().await;
    let writer_stats = writer_db.stats();

    let before_cold = reader_db.stats();
    assert_eq!(reader.read(key).await.unwrap().unwrap(), value);
    let cold = reader_db.stats() - before_cold;
    assert_eq!(cold.transactions.reads, 1);
    assert!(cold.cache.l1_misses > 0);
    assert!(cold.monitor.final_status_misses > 0);

    let before_warm = reader_db.stats();
    let reader_ref = &reader;
    reader_db
        .tx(|tx| async move {
            let first = tx.read(reader_ref, key).await?.ok_or(Error::NotFound)?;
            glassdb::ensure_tx!(
                first == value,
                Error::internal(format!("first cached read returned {first:?}"))
            );
            let second = tx.read(reader_ref, key).await?.ok_or(Error::NotFound)?;
            glassdb::ensure_tx!(
                second == value,
                Error::internal(format!("second cached read returned {second:?}"))
            );
            Ok(())
        })
        .await
        .unwrap();
    let warm = reader_db.stats() - before_warm;
    assert_eq!(warm.transactions.reads, 1);
    assert!(warm.cache.l1_hits > 0);
    assert!(warm.monitor.final_status_hits > 0);
    assert_eq!(warm.monitor.final_status_misses, 0);

    let before_stale = reader_db.stats();
    assert_eq!(
        reader
            .read_stale(key, std::time::Duration::MAX)
            .await
            .unwrap()
            .unwrap(),
        value
    );
    let stale = reader_db.stats() - before_stale;
    assert_eq!(stale.transactions.reads, 0);
    assert!(stale.cache.l1_hits > 0);
    assert!(stale.monitor.final_status_hits > 0);
    assert_eq!(stale.backend.obj_reads, 0);

    reader.delete(key).await.unwrap();
    let before_deleted = reader_db.stats();
    assert!(reader.read(key).await.unwrap().is_none());
    let deleted = reader_db.stats() - before_deleted;
    assert_eq!(deleted.transactions.reads, 1);
    assert!(deleted.cache.l1_hits > 0);

    assert_eq!(writer_db.stats(), writer_stats);
    reader_db.shutdown().await;
    let final_stats = reader_db.stats();
    assert_eq!(reader_db.clone().stats(), final_stats);
    assert_eq!(reader_db.stats() - final_stats, glassdb::Stats::default());
}

#[tokio::test(start_paused = true)]
async fn point_reads_preserve_local_values_absence_and_staged_changes() {
    let db = init_db(mem()).await;
    let collection = create_top(&db, b"point-accesses").await;
    collection.write(b"present", b"old").await.unwrap();
    let before = db.stats();
    let collection_ref = &collection;

    let values = db
        .tx(|tx| async move {
            let mut values = Vec::new();
            for key in [b"present".as_slice(), b"missing"] {
                values.push(tx.read(collection_ref, key).await?);
                values.push(tx.read(collection_ref, key).await?);
                tx.write(collection_ref, key, b"staged")?;
                values.push(tx.read(collection_ref, key).await?);
                tx.delete(collection_ref, key)?;
                values.push(tx.read(collection_ref, key).await?);
            }
            Ok(values)
        })
        .await
        .unwrap();

    assert_eq!(
        values,
        vec![
            Some(b"old".to_vec()),
            Some(b"old".to_vec()),
            Some(b"staged".to_vec()),
            None,
            None,
            None,
            Some(b"staged".to_vec()),
            None,
        ]
    );
    let delta = db.stats() - before;
    assert_eq!(delta.transactions.reads, 2);
    assert_eq!(delta.transactions.writes, 2);
    assert!(collection.read(b"present").await.unwrap().is_none());
    assert!(collection.read(b"missing").await.unwrap().is_none());
    db.shutdown().await;
}
// `Database::diagnostics` smoke test: a fresh Database has no coordinator
// state, and the typed snapshot can be rendered after normal activity.
#[tokio::test(start_paused = true)]
async fn diagnostics_returns_typed_snapshot() {
    let db = init_db(mem()).await;

    // A fresh Database has no coordination state.
    let idle = db.diagnostics();
    assert!(idle.coordinator_dedup.is_empty(), "fresh dedup: {idle:?}");

    // After running a transaction, the snapshot is still callable and renders
    // through the Display impl; the schema (typed fields) is the contract we
    // care about here.
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    let coll_ref = &coll;
    db.tx(|tx| async move {
        tx.write(coll_ref, b"k1", b"v1")?;
        Ok(())
    })
    .await
    .unwrap();

    let diag = db.diagnostics();
    let rendered = format!("{diag}");
    assert!(
        rendered.starts_with("Diagnostics:"),
        "unexpected dump: {rendered}",
    );
}
