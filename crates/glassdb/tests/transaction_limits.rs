//! Configurable transaction admission limits through the public interface.

use std::sync::Arc;
use std::time::Duration;

use glassdb::{Database, Error, KeyScan, TransactionLimits, memory::MemoryBackend};

fn expect_limit<T>(result: Result<T, Error>, resource: &'static str, limit: usize) {
    assert!(
        matches!(result, Err(Error::LimitExceeded { resource: actual, limit: max })
        if actual == resource && max == limit)
    );
}

#[tokio::test(start_paused = true)]
async fn oversized_write_inputs_are_rejected_before_backend_work() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_key_bytes: 3,
            max_value_bytes: 5,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    let before = db.stats().backend;

    expect_limit(c.write(b"long", b"v").await, "logical key bytes", 3);
    expect_limit(c.write(b"key", b"longer").await, "value bytes", 5);
    assert_eq!(db.stats().backend, before);

    c.write(b"key", b"value").await.unwrap();
    assert_eq!(
        c.read(b"key").await.unwrap().as_deref(),
        Some(&b"value"[..])
    );
}

#[tokio::test(start_paused = true)]
async fn handled_size_error_does_not_replace_a_staged_value() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_value_bytes: 3,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    db.tx(|tx| async move {
        tx.write(c, b"k", b"old")?;
        glassdb::ensure_tx!(
            matches!(tx.write(c, b"k", b"long"), Err(Error::LimitExceeded { .. })),
            Error::internal("oversized write was accepted")
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"old"[..]));
}

#[tokio::test(start_paused = true)]
async fn size_error_based_on_an_invalidated_read_replays_the_body() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_value_bytes: 1,
            max_operations: 2,
            max_write_bytes: 2,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    c.write(b"k", b"0").await.unwrap();

    db.tx(|tx| async move {
        let value = tx.read(c, b"k").await?.ok_or(Error::NotFound)?;
        if value == b"0" {
            c.write(b"k", b"1").await?;
            return tx.write(c, b"k", b"too long");
        }
        tx.write(c, b"k", b"2")
    })
    .await
    .unwrap();
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert!(db.stats().transactions.replays > 0);
}

#[tokio::test(start_paused = true)]
async fn defaults_bound_key_and_value_inputs() {
    let db = Database::open("limits", MemoryBackend::new())
        .await
        .unwrap();
    let c = db.root_collection();
    expect_limit(
        c.write(&vec![0; 4 * 1024 + 1], b"v").await,
        "logical key bytes",
        4 * 1024,
    );
    expect_limit(
        c.write(b"k", &vec![0; 1024 * 1024 + 1]).await,
        "value bytes",
        1024 * 1024,
    );
}

#[tokio::test(start_paused = true)]
async fn smaller_write_key_limits_allow_reads_and_deletes_of_existing_keys() {
    for max_key_bytes in [TransactionLimits::default().max_key_bytes, 0] {
        let backend = Arc::new(MemoryBackend::new());
        let writer = Database::builder("limits", backend.clone())
            .transaction_limits(TransactionLimits {
                max_key_bytes: 8 * 1024,
                ..TransactionLimits::default()
            })
            .open()
            .await
            .unwrap();
        let key = vec![b'k'; 5 * 1024];
        writer
            .root_collection()
            .write(&key, b"existing")
            .await
            .unwrap();
        writer.shutdown().await;

        let db = Database::builder("limits", backend)
            .transaction_limits(TransactionLimits {
                max_key_bytes,
                ..TransactionLimits::default()
            })
            .open()
            .await
            .unwrap();
        let c = db.root_collection();
        assert_eq!(
            c.read(&key).await.unwrap().as_deref(),
            Some(b"existing".as_slice())
        );
        assert_eq!(
            c.read_stale(&key, Duration::ZERO).await.unwrap().as_deref(),
            Some(b"existing".as_slice())
        );
        expect_limit(
            c.write(&key, b"replacement").await,
            "logical key bytes",
            max_key_bytes,
        );
        assert_eq!(
            c.read(&key).await.unwrap().as_deref(),
            Some(b"existing".as_slice())
        );
        c.delete(&key).await.unwrap();
        assert_eq!(c.read(&key).await.unwrap(), None);
        db.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn smaller_write_key_limits_allow_scan_bounds_and_returned_cursors() {
    let backend = Arc::new(MemoryBackend::new());
    let writer = Database::builder("limits", backend.clone())
        .transaction_limits(TransactionLimits {
            max_key_bytes: 8 * 1024,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let prefix = vec![b'k'; 5 * 1024];
    let mut first_key = prefix.clone();
    first_key.push(b'a');
    let mut second_key = prefix.clone();
    second_key.push(b'b');
    for key in [&first_key, &second_key] {
        writer.root_collection().write(key, b"value").await.unwrap();
    }
    writer.shutdown().await;

    let db = Database::open("limits", backend).await.unwrap();
    let c = db.root_collection();
    let first = c.scan_keys(KeyScan::all().limit(1)).await.unwrap();
    assert_eq!(first.keys(), std::slice::from_ref(&first_key));
    let second = c
        .scan_keys(KeyScan::all().after(first.next_after().unwrap()).limit(1))
        .await
        .unwrap();
    assert_eq!(second.keys(), std::slice::from_ref(&second_key));
    let range = c
        .scan_keys(KeyScan::range(&first_key, &second_key))
        .await
        .unwrap();
    assert_eq!(range.keys(), std::slice::from_ref(&first_key));
    let prefixed = c.scan_keys(KeyScan::prefix(&prefix)).await.unwrap();
    assert_eq!(prefixed.keys(), &[first_key, second_key]);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn zero_limits_allow_empty_inputs_and_existing_values_remain_readable() {
    let backend = Arc::new(MemoryBackend::new());
    let writer = Database::open("limits", backend.clone()).await.unwrap();
    writer
        .root_collection()
        .write(b"", b"existing")
        .await
        .unwrap();
    writer.shutdown().await;

    let db = Database::builder("limits", backend)
        .transaction_limits(TransactionLimits {
            max_key_bytes: 0,
            max_value_bytes: 0,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    assert_eq!(
        c.read(b"").await.unwrap().as_deref(),
        Some(&b"existing"[..])
    );
    expect_limit(c.write(b"k", b"").await, "logical key bytes", 0);
    expect_limit(c.write(b"", b"v").await, "value bytes", 0);
    c.write(b"", b"").await.unwrap();
    assert_eq!(c.read(b"").await.unwrap(), Some(Vec::new()));
    c.delete(b"").await.unwrap();
    assert_eq!(c.read(b"").await.unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn concurrent_reads_share_the_operation_limit() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_operations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let (first, second) = db
        .tx(|tx| async move { Ok(futures::join!(tx.read(c, b"a"), tx.read(c, b"b"))) })
        .await
        .unwrap();
    assert_eq!(first.unwrap(), None);
    expect_limit(second, "transaction operations", 1);
}

#[tokio::test(start_paused = true)]
async fn collection_operations_scans_and_repeated_accesses_share_one_limit() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_operations: 4,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            tx.collection_exists(c, b"child").await?;
            tx.scan_keys(c, KeyScan::all()).await?;
            tx.write(c, b"k", b"v")?;
            tx.read(c, b"k").await?;
            Ok(tx.delete(c, b"k"))
        })
        .await
        .unwrap();
    expect_limit(rejected, "transaction operations", 4);
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

#[tokio::test(start_paused = true)]
async fn zero_operations_rejects_access_before_backend_work() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_operations: 0,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    let before = db.stats().backend;
    expect_limit(c.read(b"k").await, "transaction operations", 0);
    expect_limit(c.write(b"k", b"v").await, "transaction operations", 0);
    expect_limit(
        c.scan_keys(KeyScan::all()).await,
        "transaction operations",
        0,
    );
    expect_limit(
        c.create_collection(b"child").await,
        "transaction operations",
        0,
    );
    assert_eq!(db.stats().backend, before);
}

#[tokio::test(start_paused = true)]
async fn rejected_collection_handles_still_count_as_operations() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_operations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let other = Database::open("other", MemoryBackend::new()).await.unwrap();
    let foreign = &other.root_collection();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            glassdb::ensure_tx!(
                matches!(tx.read(foreign, b"k").await, Err(Error::InvalidInput(_))),
                Error::internal("foreign collection handle was accepted")
            );
            Ok(tx.read(c, b"k").await)
        })
        .await
        .unwrap();
    expect_limit(rejected, "transaction operations", 1);
}

#[tokio::test(start_paused = true)]
async fn write_byte_limit_counts_keys_and_values_and_preserves_admitted_writes() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_write_bytes: 7,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            tx.write(c, b"a", b"abc")?;
            tx.write(c, b"b", b"xy")?;
            Ok(tx.write(c, b"c", b"z"))
        })
        .await
        .unwrap();
    expect_limit(rejected, "transaction write bytes", 7);
    assert_eq!(c.read(b"a").await.unwrap().as_deref(), Some(&b"abc"[..]));
    assert_eq!(c.read(b"b").await.unwrap().as_deref(), Some(&b"xy"[..]));
    assert_eq!(c.read(b"c").await.unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn replacements_and_deletes_consume_the_write_byte_budget() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_write_bytes: 5,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            tx.write(c, b"k", b"a")?;
            tx.write(c, b"k", b"b")?;
            tx.delete(c, b"k")?;
            Ok(tx.write(c, b"k", b"c"))
        })
        .await
        .unwrap();
    expect_limit(rejected, "transaction write bytes", 5);
    assert_eq!(c.read(b"k").await.unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn failed_write_admission_does_not_consume_bytes() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_write_bytes: 3,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    db.tx(|tx| async move {
        glassdb::ensure_tx!(
            matches!(tx.write(c, b"k", b"abc"), Err(Error::LimitExceeded { .. })),
            Error::internal("write byte limit was not enforced")
        );
        tx.write(c, b"k", b"ok")
    })
    .await
    .unwrap();
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"ok"[..]));
}

#[tokio::test(start_paused = true)]
async fn write_byte_budget_is_reset_when_the_body_replays() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_write_bytes: 2,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    c.write(b"k", b"0").await.unwrap();
    db.tx(|tx| async move {
        let value = tx.read(c, b"k").await?.ok_or(Error::NotFound)?;
        tx.write(c, b"k", b"2")?;
        if value == b"0" {
            c.write(b"k", b"1").await?;
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"2"[..]));
    assert!(db.stats().transactions.replays > 0);
}

#[tokio::test(start_paused = true)]
async fn collection_reservation_limit_preserves_admitted_changes() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_collection_reservations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            let first = tx.create_collection(c, b"first").await?;
            let (same, _) = tx.create_collection_if_absent(c, b"first").await?;
            tx.write(&first, b"k", b"v")?;
            let value = tx.read(&same, b"k").await?;
            glassdb::ensure_tx!(
                value.as_deref() == Some(&b"v"[..]),
                Error::internal("repeated creation changed the collection identity")
            );
            Ok(tx.create_collection(c, b"second").await)
        })
        .await
        .unwrap();
    expect_limit(rejected, "collection reservations", 1);
    assert!(c.collection_exists(b"first").await.unwrap());
    assert!(!c.collection_exists(b"second").await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn staged_drops_do_not_refund_collection_reservations() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_collection_reservations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    let rejected = db
        .tx(|tx| async move {
            let first = tx.create_collection(c, b"first").await?;
            tx.drop_collection(&first).await?;
            tx.create_collection(c, b"second").await
        })
        .await;
    expect_limit(rejected, "collection reservations", 1);
    assert!(!c.collection_exists(b"first").await.unwrap());
    assert!(!c.collection_exists(b"second").await.unwrap());
}

#[tokio::test(start_paused = true)]
async fn body_replay_can_reuse_a_collection_reservation_at_capacity() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_collection_reservations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    c.write(b"guard", b"0").await.unwrap();
    let child = db
        .tx(|tx| async move {
            let value = tx.read(c, b"guard").await?.ok_or(Error::NotFound)?;
            let child = tx.create_collection(c, b"child").await?;
            tx.write(&child, b"k", &value)?;
            if value == b"0" {
                c.write(b"guard", b"1").await?;
            }
            Ok(child)
        })
        .await
        .unwrap();
    assert_eq!(child.read(b"k").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert!(db.stats().transactions.replays > 0);
}

#[tokio::test(start_paused = true)]
async fn body_replay_cannot_accumulate_new_collection_reservations() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_collection_reservations: 1,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();
    c.write(b"guard", b"0").await.unwrap();
    let result = db
        .tx(|tx| async move {
            let value = tx.read(c, b"guard").await?.ok_or(Error::NotFound)?;
            let name = if value == b"0" {
                b"first".as_slice()
            } else {
                b"second".as_slice()
            };
            tx.create_collection(c, name).await?;
            if value == b"0" {
                c.write(b"guard", b"1").await?;
            }
            Ok(())
        })
        .await;
    expect_limit(result, "collection reservations", 1);
    assert!(!c.collection_exists(b"first").await.unwrap());
    assert!(!c.collection_exists(b"second").await.unwrap());
    assert!(db.stats().transactions.replays > 0);
}

#[tokio::test(start_paused = true)]
async fn zero_collection_reservations_allows_existing_bindings() {
    let backend = Arc::new(MemoryBackend::new());
    let writer = Database::open("limits", backend.clone()).await.unwrap();
    writer
        .root_collection()
        .create_collection(b"existing")
        .await
        .unwrap();
    writer.shutdown().await;
    let db = Database::builder("limits", backend)
        .transaction_limits(TransactionLimits {
            max_collection_reservations: 0,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    c.create_collection_if_absent(b"existing").await.unwrap();
    expect_limit(
        c.create_collection(b"new").await,
        "collection reservations",
        0,
    );
    c.open_collection(b"existing")
        .await
        .unwrap()
        .drop_collection()
        .await
        .unwrap();
}

/// Returns the key of index `i` with `len` bytes. All these keys are between
/// `m` and `n`.
fn bulk_key(i: usize, len: usize) -> Vec<u8> {
    let mut key = format!("m{i:08}").into_bytes();
    key.resize(len, b'x');
    key
}

/// Writes `count` new keys of `key_len` bytes in one transaction.
async fn create_keys(db: &Database, count: usize, key_len: usize) -> Result<(), Error> {
    let c = &db.root_collection();
    db.tx(|tx| async move {
        for i in 0..count {
            tx.write(c, &bulk_key(i, key_len), b"v")?;
        }
        Ok(())
    })
    .await
}

/// Writes keys around the range of the bulk keys. A split divides a leaf only
/// at an existing key, so no split can divide the bulk keys.
async fn write_keys_outside_the_bulk_range(db: &Database) {
    for key in [b"a", b"b", b"z"] {
        db.root_collection().write(key, b"v").await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn one_transaction_can_create_more_keys_than_an_ordinary_leaf_holds() {
    let db = Database::builder("limits", MemoryBackend::new())
        .open()
        .await
        .unwrap();
    write_keys_outside_the_bulk_range(&db).await;

    create_keys(&db, 2500, 400).await.unwrap();

    let c = db.root_collection();
    for i in [0, 2499] {
        assert_eq!(
            c.read(&bulk_key(i, 400)).await.unwrap().as_deref(),
            Some(&b"v"[..])
        );
    }
}

#[tokio::test(start_paused = true)]
async fn one_transaction_can_read_more_absent_keys_than_an_ordinary_leaf_holds() {
    let db = Database::builder("limits", MemoryBackend::new())
        .open()
        .await
        .unwrap();
    let root = &db.root_collection();
    // The write in another collection makes the transaction lock its reads.
    let other = &root.create_collection_if_absent(b"other").await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(60),
        db.tx(|tx| async move {
            for i in 0..2500 {
                if tx.read(root, &bulk_key(i, 400)).await?.is_some() {
                    return Err(Error::internal("the bulk key exists"));
                }
            }
            tx.write(other, b"k", b"v")
        }),
    )
    .await;

    result.expect("the locked reads must fit the leaf").unwrap();
    assert_eq!(other.read(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

/// Limits that allow a stage larger than the content limit of the default
/// hard cap.
fn limits_over_the_hard_cap() -> TransactionLimits {
    TransactionLimits {
        max_operations: 16_384,
        max_key_access_bytes: 16 * 1024 * 1024,
        ..TransactionLimits::default()
    }
}

#[tokio::test(start_paused = true)]
async fn creates_that_no_split_can_divide_fail_without_waiting_for_one() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(limits_over_the_hard_cap())
        .open()
        .await
        .unwrap();
    write_keys_outside_the_bulk_range(&db).await;

    let started = tokio::time::Instant::now();
    let result = create_keys(&db, 8000, 400).await;
    assert!(
        matches!(result, Err(Error::InvalidInput(_))),
        "got {result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the transaction waited {:?} for a split that cannot help",
        started.elapsed()
    );
    let c = db.root_collection();
    assert_eq!(c.read(&bulk_key(0, 400)).await.unwrap(), None);
}

#[tokio::test(start_paused = true)]
async fn creates_that_splits_can_divide_wait_for_the_splits() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(limits_over_the_hard_cap())
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    // Existing keys between the new keys let the splits divide them.
    for i in (0..8000).step_by(100) {
        c.write(&bulk_key(i, 9), b"v").await.unwrap();
    }

    create_keys(&db, 8000, 400).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn key_access_byte_limit_counts_reads_writes_and_deletes() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_key_access_bytes: 6,
            ..TransactionLimits::default()
        })
        .open()
        .await
        .unwrap();
    let c = &db.root_collection();

    let result = db
        .tx(|tx| async move {
            tx.read(c, b"ab").await?;
            tx.write(c, b"cd", b"v")?;
            tx.delete(c, b"ef")?;
            expect_limit(tx.read(c, b"g").await, "transaction key access bytes", 6);
            Ok(())
        })
        .await;

    result.unwrap();
    assert_eq!(c.read(b"cd").await.unwrap().as_deref(), Some(&b"v"[..]));
}
