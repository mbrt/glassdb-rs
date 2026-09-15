//! Exact public-history fuzz target for bounded transactions.
//!
//! Each input supplies transaction programs plus independent scheduling,
//! transport-fault, crash, and simulated-cache media tapes. Every history is
//! checked against the implementation-independent sequential specification in
//! both cache modes. Programs include point and concurrent-group reads plus
//! normalized bounded membership scans, and shared collection lifecycle
//! operations. Collection creation, deletion, nested children, and values are
//! checked atomically with key effects. Long-lived snapshot reads are not generated.
//!
//! Four client tasks each run up to three transactions; empty programs are allowed.
//! Adjacent pairs share a database instance, its caches and transport, and its
//! crash/restart lifetime. The checker keeps the four logical client identities
//! and their operation order distinct.
//! Clients continue after admissible public errors without replaying the failed
//! operation. In-doubt effects remain optional in the exact history check.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::HistoryWorkload>(data));
