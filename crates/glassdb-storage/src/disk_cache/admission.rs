//! Persistent-cache admission and pressure bookkeeping.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

// Bound admission metadata independently of the configured cache capacity.
const FILTER_BYTES: usize = 4 * 1024 * 1024;
// Aging prevents saturated counters from preserving stale popularity forever.
const FILTER_HIT_EPOCH: u64 = 1 << 20;

// Two bits distinguish the three histories needed by second-chance admission:
// unseen, seen once, and seen at least twice. The fourth state stays unused.
const FILTER_COUNTER_BITS: usize = 2;
const FILTER_COUNTERS_PER_CELL: usize = u8::BITS as usize / FILTER_COUNTER_BITS;
const FILTER_COUNTER_MASK: u8 = (1 << FILTER_COUNTER_BITS) - 1;
const SEEN_ONCE: u8 = 1;
const SEEN_AT_LEAST_TWICE: u8 = 2;

// SplitMix64's Weyl increment is a stable salt that makes the mixed counter a
// separate mapping from the raw fingerprint counter.
const SECOND_COUNTER_SALT: u64 = 0x9e37_79b9_7f4a_7c15;
// Use SplitMix64's published avalanche multipliers instead of maintaining
// cache-specific mixing constants: https://prng.di.unimi.it/splitmix64.c
const SPLITMIX64_MULTIPLIER_1: u64 = 0xbf58_476d_1ce4_e5b9;
const SPLITMIX64_MULTIPLIER_2: u64 = 0x94d0_49bb_1331_11eb;

const OPTIONAL_QUEUE_ITEMS: usize = 3072;
const MAX_QUEUED_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct Admission {
    filter: Arc<HitFilter>,
    promotions: Mutex<HashSet<Arc<str>>>,
    queued_payload_bytes: AtomicU64,
    optional_queued: AtomicUsize,
}

impl Admission {
    pub(super) fn new(filter: Arc<HitFilter>) -> Self {
        Self {
            filter,
            promotions: Mutex::new(HashSet::new()),
            queued_payload_bytes: AtomicU64::new(0),
            optional_queued: AtomicUsize::new(0),
        }
    }

    pub(super) fn observe_hit(&self, fingerprint: u64) -> bool {
        self.filter.observe(fingerprint)
    }

    pub(super) fn reserve_payload(self: &Arc<Self>, bytes: u64) -> Option<PayloadReservation> {
        self.queued_payload_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= MAX_QUEUED_PAYLOAD_BYTES)
            })
            .ok()?;
        Some(PayloadReservation {
            admission: self.clone(),
            bytes,
        })
    }

    pub(super) fn reserve_promotion(&self, path: &Arc<str>) -> bool {
        let mut queued = self.promotions.lock().unwrap();
        if queued.contains(path.as_ref()) || queued.len() >= OPTIONAL_QUEUE_ITEMS {
            return false;
        }
        queued.insert(path.clone());
        true
    }

    pub(super) fn remove_promotion(&self, path: &str) {
        self.promotions.lock().unwrap().remove(path);
    }

    pub(super) fn reserve_optional(&self) -> bool {
        self.optional_queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < OPTIONAL_QUEUE_ITEMS).then_some(current + 1)
            })
            .is_ok()
    }

    pub(super) fn release_optional(&self) {
        self.optional_queued.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct PayloadReservation {
    admission: Arc<Admission>,
    bytes: u64,
}

impl Drop for PayloadReservation {
    fn drop(&mut self) {
        self.admission
            .queued_payload_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) struct HitFilter {
    cells: Box<[AtomicU8]>,
    hits: AtomicU64,
    segment_reinitializations: AtomicUsize,
    resetting: AtomicBool,
}

impl HitFilter {
    pub(super) fn new() -> Self {
        let cells = std::iter::repeat_with(|| AtomicU8::new(0))
            .take(FILTER_BYTES)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            cells,
            hits: AtomicU64::new(0),
            segment_reinitializations: AtomicUsize::new(0),
            resetting: AtomicBool::new(false),
        }
    }

    pub(super) fn note_segment_reinitialized(&self, segment_count: usize) {
        let threshold = segment_count.div_ceil(2);
        let count = self
            .segment_reinitializations
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if count >= threshold {
            self.reset();
        }
    }

    fn observe(&self, fingerprint: u64) -> bool {
        let counters = FILTER_BYTES * FILTER_COUNTERS_PER_CELL;
        let first = fingerprint as usize % counters;
        let second = splitmix64_mix(fingerprint ^ SECOND_COUNTER_SALT) as usize % counters;
        let first_before = self.increment(first);
        let second_before = if first == second {
            first_before
        } else {
            self.increment(second)
        };
        // Requiring both counters to have a prior hit avoids admitting a path
        // merely because one of its positions collided with another path.
        let before = first_before.min(second_before);
        let hits = self.hits.fetch_add(1, Ordering::Relaxed) + 1;
        if hits >= FILTER_HIT_EPOCH {
            self.reset();
        }
        before == SEEN_ONCE
    }

    fn increment(&self, counter: usize) -> u8 {
        let cell_index = counter / FILTER_COUNTERS_PER_CELL;
        let shift = (counter % FILTER_COUNTERS_PER_CELL) * FILTER_COUNTER_BITS;
        let mask = FILTER_COUNTER_MASK << shift;
        let cell = &self.cells[cell_index];
        let mut current = cell.load(Ordering::Relaxed);
        loop {
            let before = (current & mask) >> shift;
            let after = before.saturating_add(1).min(SEEN_AT_LEAST_TWICE);
            let next = (current & !mask) | (after << shift);
            match cell.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return before,
                Err(actual) => current = actual,
            }
        }
    }

    fn reset(&self) {
        if self
            .resetting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        for cell in &self.cells {
            cell.store(0, Ordering::Relaxed);
        }
        self.hits.store(0, Ordering::Relaxed);
        self.segment_reinitializations.store(0, Ordering::Relaxed);
        self.resetting.store(false, Ordering::Release);
    }
}

/// Produces a stable, well-dispersed permutation of a 64-bit value.
fn splitmix64_mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(SPLITMIX64_MULTIPLIER_1);
    value ^= value >> 27;
    value = value.wrapping_mul(SPLITMIX64_MULTIPLIER_2);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::HitFilter;

    #[test]
    fn second_chance_filter_emits_once_on_the_second_hit_and_resets() {
        let filter = HitFilter::new();
        assert!(!filter.observe(42));
        assert!(filter.observe(42));
        assert!(!filter.observe(42));

        filter.note_segment_reinitialized(4);
        filter.note_segment_reinitialized(4);
        assert!(!filter.observe(42));
        assert!(filter.observe(42));
    }
}
