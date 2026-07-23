//! Replays the committed fuzz corpora through the same harnesses as the fuzzer,
//! on stable (no nightly/cargo-fuzz/sanitizer). This is a regression guard:
//! every input the fuzzer has ever found interesting must still satisfy its
//! invariant (the serializability bound for `concurrent_tx`, the ring invariant
//! for `cycle`, the sorted-listing + membership bound for `membership`, and
//! the modeled transaction-API states for `api_correctness`).
//!
//! Like the other simulation self-checks, this only builds under the in-repo
//! deterministic executor with the `sim` harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test --profile sim-test -p glassdb --features sim
//! ```
#![cfg(all(sim, feature = "sim"))]

use std::path::{Path, PathBuf};

use glassdb::middleware::{OpRecord, first_divergence};
use glassdb::sim::{ApiWorkload, CycleWorkload, MembershipWorkload, RmwWorkload, record_input};
use rayon::prelude::*;

type ReplayFn = fn(&[u8]) -> Vec<OpRecord>;

fn divergence_error(label: &str, first: &[OpRecord], second: &[OpRecord]) -> Option<String> {
    if let Some((idx, a, b)) = first_divergence(first, second) {
        return Some(format!(
            "{label}: op stream diverged at index {idx}\n  \
             run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
            first.len(),
            second.len(),
        ));
    }
    None
}

fn replay_corpus_file(path: &Path, replay: ReplayFn) -> Result<(), String> {
    let data = std::fs::read(path)
        .map_err(|error| format!("read corpus file {}: {error}", path.display()))?;
    let first = std::panic::catch_unwind(|| replay(&data))
        .map_err(|_| format!("corpus replay failed for {}", path.display()))?;
    let second = std::panic::catch_unwind(|| replay(&data))
        .map_err(|_| format!("second corpus replay failed for {}", path.display()))?;
    if let Some(error) = divergence_error(
        &format!("corpus replay for {}", path.display()),
        &first,
        &second,
    ) {
        return Err(error);
    }
    Ok(())
}

/// Replays every committed input under `fuzz/corpus/<target>` through `replay`,
/// which panics on any invariant violation, and compares two back-to-back
/// recorded op streams. On failure the offending file is named: it is the exact
/// libFuzzer reproducer for `cargo +nightly fuzz run <target> <file>`.
fn replay_committed_corpus(target: &str, replay: ReplayFn) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fuzz/corpus")
        .join(target);
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read corpus dir") {
        let path = entry.expect("read corpus entry").path();
        if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    assert!(!paths.is_empty(), "no corpus files under {}", dir.display());

    if let Err(error) = paths
        .par_iter()
        .try_for_each(|path| replay_corpus_file(path, replay))
    {
        panic!("{error}");
    }
}

#[test]
fn replays_committed_corpus() {
    replay_committed_corpus("concurrent_tx", record_input::<RmwWorkload>);
}

#[test]
fn replays_committed_cycle_corpus() {
    replay_committed_corpus("cycle", record_input::<CycleWorkload>);
}

#[test]
fn replays_committed_membership_corpus() {
    replay_committed_corpus("membership", record_input::<MembershipWorkload>);
}

#[test]
fn replays_committed_api_correctness_corpus() {
    replay_committed_corpus("api_correctness", record_input::<ApiWorkload>);
}
