//! The randomness seam shared by the data types that mint high-entropy
//! identifiers (transaction IDs and B-link node tokens).
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
