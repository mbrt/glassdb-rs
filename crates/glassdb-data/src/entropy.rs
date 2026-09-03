//! Data-type entropy helpers.

use glassdb_concurr::entropy::fill_bytes;

/// Randomizes `values` using the same deterministic-under-simulation entropy as
/// transaction identities and node tokens.
pub fn shuffle<T>(values: &mut [T]) {
    for upper in (1..values.len()).rev() {
        let mut bytes = [0; 8];
        fill_bytes(&mut bytes);
        let index = (u64::from_le_bytes(bytes) % (upper as u64 + 1)) as usize;
        values.swap(upper, index);
    }
}
