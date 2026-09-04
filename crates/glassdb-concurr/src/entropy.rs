//! Process and simulation entropy.
//!
//! Callers use the same interface in every build. Native execution draws from
//! the process RNG; simulation draws from the active deterministic executor's
//! seeded stream.

/// Fills `bytes` from the active entropy source.
pub fn fill_bytes(bytes: &mut [u8]) {
    #[cfg(sim)]
    {
        crate::exec::executor::fill_random(bytes);
    }
    #[cfg(not(sim))]
    {
        use rand::Rng;
        rand::rng().fill_bytes(bytes);
    }
}

/// Samples a uniformly distributed value in `[0, 1)` from the active entropy
/// source.
pub fn uniform_unit() -> f64 {
    #[cfg(sim)]
    {
        let mut bytes = [0; 8];
        fill_bytes(&mut bytes);
        ((u64::from_le_bytes(bytes) >> 11) as f64) / ((1u64 << 53) as f64)
    }
    #[cfg(not(sim))]
    {
        use rand::RngExt;
        rand::rng().random::<f64>()
    }
}

#[cfg(all(test, sim))]
mod sim_tests {
    use super::*;

    #[test]
    fn seeded_fill_and_unit_draws_share_one_stream() {
        let (prefix, unit, suffix) =
            crate::exec::block_on_with(crate::exec::TapeScheduler::new(Vec::new()), 7, async {
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
