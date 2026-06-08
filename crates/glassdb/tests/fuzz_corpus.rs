//! Replays the committed fuzz corpora through the same harnesses as the fuzzer,
//! on stable (no nightly/cargo-fuzz/sanitizer). This is a regression guard:
//! every input the fuzzer has ever found interesting must still satisfy its
//! invariant (the serializability bound for `concurrent_tx`, the ring invariant
//! for `cycle`).
//!
//! Like the other simulation self-checks, this only builds under the in-repo
//! deterministic executor with the `sim` harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim
//! ```
#![cfg(all(sim, feature = "sim"))]

use std::path::PathBuf;

use glassdb::sim::{replay_cycle_input, replay_fuzz_input};

/// Replays every committed input under `fuzz/corpus/<target>` through `replay`,
/// which panics on any invariant violation. On failure the offending file is
/// named: it is the exact libFuzzer reproducer for
/// `cargo +nightly fuzz run <target> <file>`.
fn replay_committed_corpus(target: &str, replay: fn(&[u8])) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz/corpus")
        .join(target);
    let mut count = 0usize;
    for entry in std::fs::read_dir(&dir).expect("read corpus dir") {
        let path = entry.expect("read corpus entry").path();
        if !path.is_file() {
            continue;
        }
        let data = std::fs::read(&path).expect("read corpus file");
        if std::panic::catch_unwind(|| replay(&data)).is_err() {
            panic!("corpus replay failed for {}", path.display());
        }
        count += 1;
    }
    assert!(count > 0, "no corpus files under {}", dir.display());
}

#[test]
fn replays_committed_corpus() {
    replay_committed_corpus("concurrent_tx", replay_fuzz_input);
}

#[test]
fn replays_committed_cycle_corpus() {
    replay_committed_corpus("cycle", replay_cycle_input);
}
