//! Replays the committed persistent-cache corpus through its storage-layer harness.
#![cfg(all(sim, feature = "sim"))]

use std::path::{Path, PathBuf};

use glassdb_storage::sim::disk_cache::record_disk_cache_input;
use rayon::prelude::*;

fn replay_corpus_file(path: &Path) -> Result<(), String> {
    let data = std::fs::read(path)
        .map_err(|error| format!("read corpus file {}: {error}", path.display()))?;
    let first = std::panic::catch_unwind(|| record_disk_cache_input(&data))
        .map_err(|_| format!("corpus replay failed for {}", path.display()))?;
    let second = std::panic::catch_unwind(|| record_disk_cache_input(&data))
        .map_err(|_| format!("second corpus replay failed for {}", path.display()))?;
    if first != second {
        return Err(format!(
            "corpus replay diverged for {}\n  run 1: {first:?}\n  run 2: {second:?}",
            path.display()
        ));
    }
    Ok(())
}

#[test]
fn replays_committed_disk_cache_corpus() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/disk_cache");
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read disk-cache corpus dir") {
        let path = entry.expect("read disk-cache corpus entry").path();
        if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    assert!(
        !paths.is_empty(),
        "no disk-cache corpus files under {}",
        dir.display()
    );

    if let Err(error) = paths
        .par_iter()
        .try_for_each(|path| replay_corpus_file(path))
    {
        panic!("{error}");
    }
}
