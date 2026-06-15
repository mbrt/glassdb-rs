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

use glassdb::middleware::{OpRecord, first_divergence};
use glassdb::sim::{record_cycle_input, record_fuzz_input};

fn assert_no_divergence(label: &str, first: &[OpRecord], second: &[OpRecord]) {
    if let Some((idx, a, b)) = first_divergence(first, second) {
        panic!(
            "{label}: op stream diverged at index {idx}\n  \
             run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
            first.len(),
            second.len(),
        );
    }
}

/// Replays every committed input under `fuzz/corpus/<target>` through `replay`,
/// which panics on any invariant violation, and compares two back-to-back
/// recorded op streams. On failure the offending file is named: it is the exact
/// libFuzzer reproducer for `cargo +nightly fuzz run <target> <file>`.
fn replay_committed_corpus(target: &str, replay: fn(&[u8]) -> Vec<OpRecord>) {
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
        let first = match std::panic::catch_unwind(|| replay(&data)) {
            Ok(log) => log,
            Err(_) => panic!("corpus replay failed for {}", path.display()),
        };
        let second = match std::panic::catch_unwind(|| replay(&data)) {
            Ok(log) => log,
            Err(_) => panic!("second corpus replay failed for {}", path.display()),
        };
        assert_no_divergence(
            &format!("corpus replay for {}", path.display()),
            &first,
            &second,
        );
        count += 1;
    }
    assert!(count > 0, "no corpus files under {}", dir.display());
}

#[test]
fn replays_committed_corpus() {
    replay_committed_corpus("concurrent_tx", record_fuzz_input);
}

#[test]
fn replays_committed_cycle_corpus() {
    replay_committed_corpus("cycle", record_cycle_input);
}
