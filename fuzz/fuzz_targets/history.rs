//! Exact public-history fuzz target for bounded transactions.
//!
//! Each input supplies transaction programs plus independent scheduling,
//! transport-fault, crash, and simulated-cache media tapes. Every history is
//! checked against the implementation-independent sequential specification in
//! both cache modes. Programs include point and concurrent-group reads plus
//! normalized bounded membership scans; long-lived snapshot reads are
//! intentionally not generated.
#![no_main]
#![recursion_limit = "256"]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::HistoryWorkload>(data));
