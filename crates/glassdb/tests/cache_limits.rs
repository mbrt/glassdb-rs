//! Cache admission must not change database read and write behavior.

use std::time::Duration;

use glassdb::{Database, InlinePolicy, memory::MemoryBackend};

#[tokio::test(start_paused = true)]
async fn zero_cache_budget_preserves_reads_and_writes_without_retaining_entries() {
    for policy in [InlinePolicy::default(), InlinePolicy::none()] {
        let db = Database::builder("cachelimits", MemoryBackend::new())
            .cache_size(0)
            .inline_policy(policy)
            .open()
            .await
            .unwrap();
        let coll = db.root_collection();
        for value in [b"old".as_slice(), b"new value".as_slice()] {
            coll.write(b"key", value).await.unwrap();
            assert_eq!(coll.read(b"key").await.unwrap().as_deref(), Some(value));
            for _ in 0..2 {
                assert_eq!(
                    coll.read_stale(b"key", Duration::from_secs(60))
                        .await
                        .unwrap()
                        .as_deref(),
                    Some(value)
                );
            }
        }
        let cache = db.stats().cache;
        assert_eq!(cache.l1_hits, 0);
        assert!(cache.l1_misses > 0);
        db.shutdown().await;
    }
}
