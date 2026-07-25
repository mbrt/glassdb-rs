//! Deterministic byte-level persistent-cache fuzz target (ADR-048).
//!
//! The input has independent command, scheduler, and media-fault streams. The
//! isolated cache harness permits cache loss but rejects fabricated records,
//! invented sequence points, and access beyond the preallocated container.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| glassdb::sim::replay_disk_cache_input(data));
