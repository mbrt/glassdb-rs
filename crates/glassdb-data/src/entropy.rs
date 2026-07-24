//! The randomness seam shared by the data types that mint high-entropy
//! identifiers (transaction IDs, collection IDs, and B-link node tokens).
//!
//! In normal builds this draws from the OS via `rand`. Under the deterministic
//! simulation executor (`--cfg sim`) it draws from the run's seeded RNG instead,
//! so object keys that embed these random bytes are a deterministic function of
//! the simulation seed and replays are byte-identical.

/// Fills `b` with random bytes.
#[cfg(not(sim))]
pub(crate) fn fill_random(b: &mut [u8]) {
    use rand::Rng;
    rand::rng().fill_bytes(b);
}

/// Randomizes `values` using the same deterministic-under-simulation entropy as
/// transaction IDs and node tokens.
pub fn shuffle<T>(values: &mut [T]) {
    for upper in (1..values.len()).rev() {
        let mut bytes = [0; 8];
        fill_random(&mut bytes);
        let index = (u64::from_le_bytes(bytes) % (upper as u64 + 1)) as usize;
        values.swap(upper, index);
    }
}

/// Fills `b` with random bytes.
#[cfg(sim)]
pub(crate) fn fill_random(b: &mut [u8]) {
    // Inside the executor, draw from its seeded entropy. Outside it (e.g.
    // ordinary tokio tests built with `--cfg sim`), fall back to the OS RNG.
    if glassdb_concurr::rt::in_sim() {
        glassdb_concurr::rt::fill_random(b);
    } else {
        use rand::Rng;
        rand::rng().fill_bytes(b);
    }
}
