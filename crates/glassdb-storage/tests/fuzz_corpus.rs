//! Replays the committed persistent-cache corpus through its storage-layer harness.
#![cfg(all(sim, feature = "sim"))]

use std::path::PathBuf;

use glassdb_storage::sim::disk_cache::{DiskCacheEvent, record_disk_cache_input};

#[test]
fn replays_committed_disk_cache_corpus() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/disk_cache/seed");
    let data = std::fs::read(&path).expect("read disk-cache corpus seed");
    let first = record_disk_cache_input(&data);
    let second = record_disk_cache_input(&data);
    let enabled_opens = first
        .iter()
        .filter(|event| matches!(event, DiskCacheEvent::Opened { enabled: true, .. }))
        .count();
    assert!(
        enabled_opens >= 3,
        "disk-cache corpus did not exercise reopen: {first:?}"
    );
    for path in [0, 1] {
        assert!(
            first.iter().any(|event| matches!(
                event,
                DiskCacheEvent::Lookup {
                    path: actual,
                    record_digest: Some(_),
                    ..
                } if *actual == path
            )),
            "disk-cache corpus never returned the admitted record for path {path}"
        );
    }
    for operation in [5, 6, 7] {
        assert!(
            first.iter().any(|event| matches!(
                event,
                DiskCacheEvent::State {
                    operation: actual,
                    ..
                } if *actual == operation
            )),
            "disk-cache corpus omitted lifecycle operation {operation}"
        );
    }
    assert_eq!(
        first,
        second,
        "disk-cache corpus replay diverged for {}",
        path.display()
    );
}
