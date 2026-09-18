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
async fn oversized_inputs_are_rejected_before_backend_work() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_key_bytes: 3,
            max_value_bytes: 5,
        })
        .open()
        .await
        .unwrap();
    let c = db.root_collection();
    let before = db.stats().backend;

    expect_limit(c.write(b"long", b"v").await, "logical key bytes", 3);
    expect_limit(c.write(b"key", b"longer").await, "value bytes", 5);
    expect_limit(c.read(b"long").await, "logical key bytes", 3);
    expect_limit(c.delete(b"long").await, "logical key bytes", 3);
    expect_limit(
        c.read_stale(b"long", Duration::ZERO).await,
        "logical key bytes",
        3,
    );
    for scan in [
        KeyScan::prefix(b"long"),
        KeyScan::range(b"long", b"z"),
        KeyScan::range(b"a", b"long"),
        KeyScan::all().after(b"long"),
    ] {
        expect_limit(c.scan_keys(scan).await, "logical key bytes", 3);
    }
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
async fn size_error_based_on_a_stale_read_retries_the_body() {
    let db = Database::builder("limits", MemoryBackend::new())
        .transaction_limits(TransactionLimits {
            max_value_bytes: 1,
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
    assert!(db.stats().transactions.retries > 0);
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
