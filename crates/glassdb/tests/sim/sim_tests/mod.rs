mod api;
mod commit_recovery;
mod concurrent;
mod contended_keys;
mod coverage;
mod cycle;
mod fuzz_corpus;
mod gc;
mod history;
mod membership;
mod pct;

/// The fault intensity of the tests that check an invariant under failures.
/// It gives transport faults, one crash, and one outage
/// (`FaultOptions::from_intensity`). Intensities below 11 give no transport
/// faults.
const FAULT_INTENSITY: u8 = 64;
