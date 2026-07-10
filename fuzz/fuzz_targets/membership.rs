//! Deterministic membership/listing fuzz target (ADR-031 dynamic range sharding).
//!
//! libFuzzer bytes are split into an `rng_seed`, a [`MembershipWorkload`], a
//! [`FaultConfig`], and the remaining bytes, which are halved into a *schedule
//! tape* and a *fault tape* — the same layout as the `concurrent_tx` and `cycle`
//! targets. Each client owns a disjoint slice of a small key universe and
//! concurrently creates, deletes, and lists keys over a shared in-process
//! backend on the in-repo deterministic executor ([`glassdb::rt`], `--cfg sim`).
//! The databases open with a tiny split soft cap, so a couple of live keys
//! overflow a leaf and drive B-link leaf/root splits, right-link traversal, and
//! cross-leaf sorted listing.
//!
//! The generic harness ([`glassdb::sim::replay_input`]) asserts that every
//! committed listing is strictly sorted and drawn from the key universe, and
//! that the final key set matches the per-key membership accounting (exactly
//! with faults off; within the in-doubt bound otherwise). Any violation panics,
//! which libFuzzer reports as a crash. `cargo fuzz` overrides the
//! `fuzz/.cargo/config.toml` rustflags, so `--cfg sim --cfg tokio_unstable` must
//! be passed through the environment:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run membership <crash-file>
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;

// The decode-and-run logic lives in the generic `glassdb::sim::replay_input` so
// the committed-corpus replay test (crates/glassdb/tests/fuzz_corpus.rs)
// exercises the exact same path as the fuzzer.
fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::MembershipWorkload>(data));
