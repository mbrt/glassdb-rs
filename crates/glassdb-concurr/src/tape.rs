//! A fuzzer-guidable decision source backed by a seeded PRNG.
//!
//! [`Tape`] yields deterministic decisions (coin flips and bounded integers)
//! from a byte string supplied by the fuzzer, falling back to a seeded [`Rng`]
//! once the bytes run out. This lets the deterministic-simulation harness make
//! the *fault schedule* coverage-guidable (a byte mutation maps locally to a
//! single fault decision) while staying fully deterministic and degrading
//! gracefully to pure seed-breadth sampling when no tape is provided (e.g. PCT
//! runs pass an empty tape). It complements the executor's `TapeScheduler`,
//! which guides task *interleavings* the same way.

use crate::rng::Rng;

/// A byte-driven decision source with a seeded RNG fallback. Cheap to construct;
/// not thread-safe on its own (wrap in a `Mutex` if shared across tasks).
pub struct Tape {
    bytes: Vec<u8>,
    pos: usize,
    rng: Rng,
}

impl Tape {
    /// Creates a tape that consumes `bytes` first, then draws from a PRNG seeded
    /// with `seed`. An empty `bytes` makes every decision a pure function of the
    /// seed.
    pub fn new(bytes: Vec<u8>, seed: u64) -> Self {
        Tape {
            bytes,
            pos: 0,
            rng: Rng::new(seed),
        }
    }

    /// The next byte: from the tape while it lasts, otherwise from the seeded RNG.
    fn next_byte(&mut self) -> u8 {
        match self.bytes.get(self.pos) {
            Some(&b) => {
                self.pos += 1;
                b
            }
            None => (self.rng.next_u64() & 0xff) as u8,
        }
    }

    /// Returns true with probability `prob/256` (one byte per call). A `prob` of
    /// zero never fires.
    pub fn roll(&mut self, prob: u8) -> bool {
        prob != 0 && self.next_byte() < prob
    }

    /// Returns a value in `0..n`, or `0` when `n == 0`. Consumes as many bytes as
    /// the range needs (one per 8 bits), keeping the modulo bias negligible.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let mut acc = 0u64;
        let mut limit = n - 1;
        loop {
            acc = (acc << 8) | self.next_byte() as u64;
            if limit < 256 {
                break;
            }
            limit >>= 8;
        }
        acc % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tape_is_seed_reproducible() {
        let decisions = |seed: u64| {
            let mut t = Tape::new(Vec::new(), seed);
            (0..32).map(|_| t.roll(128)).collect::<Vec<_>>()
        };
        assert_eq!(decisions(42), decisions(42));
        assert_ne!(decisions(42), decisions(43));
    }

    #[test]
    fn tape_bytes_override_the_seed() {
        // A byte below the threshold fires; one at/above it does not, regardless
        // of the seed — so the tape, not the seed, decides while it lasts.
        let mut lo = Tape::new(vec![0, 0, 0], 999);
        assert!(lo.roll(200));
        let mut hi = Tape::new(vec![255, 255, 255], 1);
        assert!(!hi.roll(200));
    }

    #[test]
    fn below_respects_bounds_and_falls_back() {
        let mut t = Tape::new(vec![7], 1);
        assert!(t.below(4) < 4); // from the tape
        for _ in 0..100 {
            assert!(t.below(4) < 4); // from the RNG fallback
        }
        assert_eq!(t.below(0), 0);
    }
}
