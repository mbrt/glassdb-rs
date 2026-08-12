//! Process and simulation-aware entropy.
//!
//! Callers use the same interface in every build. Native execution draws from
//! the process RNG; an active deterministic executor draws from its seeded
//! stream so simulations remain replayable.

/// Fills `bytes` from the active entropy source.
pub fn fill_bytes(bytes: &mut [u8]) {
    #[cfg(sim)]
    if crate::rt::in_sim() {
        crate::rt::fill_random(bytes);
        return;
    }
    // Ordinary Tokio tests in a simulation build do not require a deterministic
    // executor and keep using the process RNG.
    fill_native(bytes);
}

/// Samples a uniformly distributed value in `[0, 1)` from the active entropy
/// source.
pub fn uniform_unit() -> f64 {
    #[cfg(sim)]
    if crate::rt::in_sim() {
        let mut bytes = [0; 8];
        fill_bytes(&mut bytes);
        return ((u64::from_le_bytes(bytes) >> 11) as f64) / ((1u64 << 53) as f64);
    }
    uniform_unit_native()
}

fn fill_native(bytes: &mut [u8]) {
    use rand::Rng;
    rand::rng().fill_bytes(bytes);
}

fn uniform_unit_native() -> f64 {
    use rand::RngExt;
    rand::rng().random::<f64>()
}

#[cfg(all(test, sim))]
mod tests {
    use super::*;

    #[test]
    fn seeded_fill_and_unit_draws_share_one_stream() {
        let (prefix, unit, suffix) =
            crate::rt::block_on_with(crate::rt::TapeScheduler::new(Vec::new()), 7, async {
                let mut prefix = [0; 10];
                fill_bytes(&mut prefix);
                let unit = uniform_unit();
                let mut suffix = [0; 8];
                fill_bytes(&mut suffix);
                (prefix, unit, suffix)
            });

        assert_eq!(prefix, [215, 13, 50, 89, 228, 225, 203, 99, 28, 102]);
        assert_eq!(unit.to_bits(), 0x3fec_d308_1017_5625);
        assert_eq!(suffix, [203, 41, 62, 103, 112, 235, 58, 149]);
    }
}
