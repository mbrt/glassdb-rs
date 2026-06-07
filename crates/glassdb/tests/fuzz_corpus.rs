//! Replays the committed `concurrent_tx` fuzz corpus through the same harness
//! as the fuzzer, on stable (no nightly/cargo-fuzz/sanitizer). This is a
//! regression guard: every input the fuzzer has ever found interesting must
//! still satisfy the serializability bound.
//!
//! Like the other simulation self-checks, this only builds under the in-repo
//! deterministic executor with the `sim` harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim
//! ```
#![cfg(all(sim, feature = "sim"))]

use std::path::PathBuf;

use glassdb::sim::replay_fuzz_input;

/// Replays every committed corpus input; `replay_fuzz_input` panics on any
/// invariant violation, which fails the test.
#[test]
fn replays_committed_corpus() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/concurrent_tx");
    let mut count = 0usize;
    for entry in std::fs::read_dir(&dir).expect("read corpus dir") {
        let path = entry.expect("read corpus entry").path();
        if !path.is_file() {
            continue;
        }
        let data = std::fs::read(&path).expect("read corpus file");
        // On failure, name the offending input: the file is the exact libFuzzer
        // reproducer for `cargo +nightly fuzz run concurrent_tx <file>`.
        if std::panic::catch_unwind(|| replay_fuzz_input(&data)).is_err() {
            panic!("corpus replay failed for {}", path.display());
        }
        count += 1;
    }
    assert!(count > 0, "no corpus files under {}", dir.display());
}
