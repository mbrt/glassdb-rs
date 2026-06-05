//! Process-wide measurement helpers for the scoring harness.
//!
//! These are the Rust analogs of the runtime introspection the Go harness uses:
//! a counting global allocator stands in for `runtime.ReadMemStats`
//! (`Mallocs`/`TotalAlloc`), and [`cpu_ns`] wraps `getrusage` exactly like the
//! Go `cpuNs`. Go's `mutexWaitNsPerTx` (from `runtime/metrics`) has no portable
//! Rust equivalent and is intentionally dropped.
//!
//! This file is part of the autoresearch fixed infrastructure: it defines how
//! the metric is measured and must NOT be modified by autoresearch experiments.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use glassdb::{Stats, DB};

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

/// A `System`-backed global allocator that counts allocations and the total
/// bytes requested, so the harness can report allocations/bytes per transaction
/// the way the Go harness reads `runtime.MemStats`. Deallocations are forwarded
/// untouched; only the cumulative request counters are tracked (matching Go's
/// monotonic `Mallocs`/`TotalAlloc`).
pub struct CountingAlloc;

// SAFETY: every method delegates to the system allocator and only touches
// relaxed atomic counters around it, preserving the allocator contract.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        p
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        // Count only the growth as freshly allocated bytes; a shrink/in-place
        // realloc is not a new allocation.
        if !p.is_null() && new_size > layout.size() {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        p
    }
}

/// A snapshot of the cumulative allocation counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct AllocSnapshot {
    pub count: u64,
    pub bytes: u64,
}

/// Reads the current cumulative allocation counters.
pub fn alloc_snapshot() -> AllocSnapshot {
    AllocSnapshot {
        count: ALLOC_COUNT.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}

/// Cumulative user + system CPU time consumed by this process, in nanoseconds.
#[cfg(unix)]
pub fn cpu_ns() -> u64 {
    use std::mem::MaybeUninit;

    // SAFETY: `getrusage` fully initializes the struct on success; we only read
    // it when the call returns 0.
    unsafe {
        let mut ru = MaybeUninit::<libc::rusage>::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) != 0 {
            return 0;
        }
        let ru = ru.assume_init();
        timeval_ns(ru.ru_utime) + timeval_ns(ru.ru_stime)
    }
}

#[cfg(unix)]
fn timeval_ns(t: libc::timeval) -> u64 {
    (t.tv_sec as u64) * 1_000_000_000 + (t.tv_usec as u64) * 1_000
}

#[cfg(not(unix))]
pub fn cpu_ns() -> u64 {
    0
}

/// The raw per-workload measurement deltas, before they are weighted into a
/// cost. Mirrors the fields the Go `measure` collects (minus mutex wait).
pub struct Sample {
    pub name: String,
    /// Backend-operation / transaction counters accumulated by the body.
    pub stats: Stats,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub cpu_ns: u64,
    pub wall_ns: u64,
}

/// Brackets a workload body: call [`Measure::begin`] right before the measured
/// transactions and [`Measure::end`] right after, then [`Measure::into_sample`].
/// Setup work done before `begin` is not counted, exactly like the Go harness.
pub struct Measure {
    name: String,
    start_stats: Stats,
    start_alloc: AllocSnapshot,
    start_cpu: u64,
    start_wall: Instant,
    sample: Option<Sample>,
}

impl Measure {
    pub fn new(name: &str) -> Self {
        Measure {
            name: name.to_string(),
            start_stats: Stats::default(),
            start_alloc: AllocSnapshot::default(),
            start_cpu: 0,
            start_wall: Instant::now(),
            sample: None,
        }
    }

    /// Snapshots all counters at the start of the measured region.
    pub fn begin(&mut self, db: &DB) {
        self.start_stats = db.stats();
        self.start_alloc = alloc_snapshot();
        self.start_cpu = cpu_ns();
        self.start_wall = Instant::now();
    }

    /// Records the deltas accumulated since [`Measure::begin`].
    pub fn end(&mut self, db: &DB) {
        let wall_ns = self.start_wall.elapsed().as_nanos() as u64;
        let cpu_ns = cpu_ns().saturating_sub(self.start_cpu);
        let alloc = alloc_snapshot();
        let stats = db.stats().sub(&self.start_stats);
        self.sample = Some(Sample {
            name: std::mem::take(&mut self.name),
            stats,
            alloc_count: alloc.count.saturating_sub(self.start_alloc.count),
            alloc_bytes: alloc.bytes.saturating_sub(self.start_alloc.bytes),
            cpu_ns,
            wall_ns,
        });
    }

    /// Consumes the measurement, returning the recorded sample. Panics if
    /// [`Measure::end`] was not called.
    pub fn into_sample(self) -> Sample {
        self.sample.expect("Measure::end was not called")
    }
}
