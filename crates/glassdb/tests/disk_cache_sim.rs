//! Narrow cross-layer checks for the simulated persistent-cache media.
#![cfg(all(sim, feature = "sim"))]

use std::path::PathBuf;
use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::rt::{TapeScheduler, block_on_with, yield_now};
use glassdb::sim::{MediaFaultProfile, SimMedia, record_disk_cache_input};
use glassdb::{Database, PersistentCacheConfig, ProtocolTiming};

const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;

fn config() -> PersistentCacheConfig {
    PersistentCacheConfig {
        directory: PathBuf::from("simulated-database-cache"),
        capacity_bytes: CAPACITY_BYTES,
    }
}

async fn open(backend: Arc<MemoryBackend>, media: SimMedia) -> Database {
    Database::builder("simcache", backend)
        .simulated_persistent_cache(config(), media)
        .deterministic_time(true)
        .protocol_timing(ProtocolTiming::simulation())
        .open()
        .await
        .unwrap()
}

#[test]
fn isolated_cache_trace_is_reproducible() {
    let input = include_bytes!("../../../fuzz/corpus/disk_cache/seed");
    let first = record_disk_cache_input(input);
    let second = record_disk_cache_input(input);
    assert_eq!(first, second);
}

#[test]
fn database_uses_cleanly_reopened_cache_and_survives_crash() {
    block_on_with(TapeScheduler::new(Vec::new()), 41, async {
        let backend = Arc::new(MemoryBackend::new());
        let media = SimMedia::new(MediaFaultProfile::Healthy, vec![0, 255, 0, 255], 41);

        let first = open(backend.clone(), media.clone()).await;
        first
            .root_collection()
            .write(b"key", b"value")
            .await
            .unwrap();
        first.shutdown().await;
        drop(first);

        // Populate L2 from a backend read, then make that admission durable.
        let warming = open(backend.clone(), media.clone()).await;
        assert_eq!(
            warming.root_collection().read(b"key").await.unwrap(),
            Some(b"value".to_vec())
        );
        warming.shutdown().await;
        drop(warming);

        let clean = open(backend.clone(), media.clone()).await;
        let before = clean.stats();
        assert_eq!(
            clean.root_collection().read(b"key").await.unwrap(),
            Some(b"value".to_vec())
        );
        let read_stats = clean.stats() - before;
        assert!(
            read_stats.cache.l2_hits > 0,
            "clean reopen did not serve any object from L2: {read_stats:?}"
        );

        media.crash();
        drop(clean);
        yield_now().await;

        let crashed = open(backend, media).await;
        assert_eq!(
            crashed.root_collection().read(b"key").await.unwrap(),
            Some(b"value".to_vec())
        );
        crashed.shutdown().await;
    });
}

#[test]
fn database_stays_available_when_cache_open_fails() {
    block_on_with(TapeScheduler::new(Vec::new()), 73, async {
        let backend = Arc::new(MemoryBackend::new());
        let media = SimMedia::new(MediaFaultProfile::Selected, vec![255], 73);
        let database = open(backend, media).await;

        database
            .root_collection()
            .write(b"key", b"value")
            .await
            .unwrap();
        assert_eq!(
            database.root_collection().read(b"key").await.unwrap(),
            Some(b"value".to_vec())
        );
        assert!(database.stats().cache.l2_errors >= 1);
        database.shutdown().await;
    });
}
