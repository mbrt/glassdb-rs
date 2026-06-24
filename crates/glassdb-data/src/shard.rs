//! Key→shard mapping for the v2 sharded coordination directory (ADR-017).
//!
//! Each collection is split into a fixed [`SHARD_COUNT`] shard objects. A key's
//! shard is `fnv1a(key) & (SHARD_COUNT - 1)`. Both [`SHARD_COUNT`] and the
//! FNV-1a mapping are part of the on-disk format: changing either remaps every
//! key, so they are format-version constants, never runtime options.

use crate::paths;

const FNV_OFFSET_32: u32 = 2166136261;
const FNV_PRIME_32: u32 = 16777619;

/// Number of shard objects per collection. Must be a power of two so the modulo
/// reduces to a bit mask. Part of the on-disk format: changing it remaps every
/// key.
pub const SHARD_COUNT: u32 = 1024;

const _: () = assert!(
    SHARD_COUNT.is_power_of_two(),
    "SHARD_COUNT must be a power of two"
);

/// Width of the zero-padded decimal shard index used in shard paths: the number
/// of digits in `SHARD_COUNT - 1` (e.g. 4 for 1024). Keeps paths a stable,
/// lexicographically ordered function of the index.
pub(crate) const SHARD_INDEX_WIDTH: usize = decimal_width(SHARD_COUNT - 1);

const fn decimal_width(mut n: u32) -> usize {
    let mut w = 1;
    while n >= 10 {
        n /= 10;
        w += 1;
    }
    w
}

/// Returns the index of the shard that owns `key`, in `0..SHARD_COUNT`.
///
/// Hashes the raw user key bytes (not the base64 path encoding) with FNV-1a, the
/// same hash used for in-memory sharding (`glassdb-concurr::shard`, ADR-001). It
/// is deterministic and stable across processes and under `--cfg sim`, so every
/// client agrees on the mapping and DST replays are byte-identical.
pub fn shard_index(key: &[u8]) -> u32 {
    fnv1a(key) & (SHARD_COUNT - 1)
}

/// Returns the storage path of the shard that owns `key` under `prefix`.
pub fn shard_path(prefix: &str, key: &[u8]) -> String {
    paths::from_shard(prefix, shard_index(key))
}

/// Inline FNV-1a 32-bit hash that avoids allocating a hasher. Mirrors the
/// constants used in `glassdb-concurr::shard`.
fn fnv1a(key: &[u8]) -> u32 {
    let mut h = FNV_OFFSET_32;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME_32);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_in_range() {
        for i in 0..5000 {
            let key = format!("key-{i}");
            assert!(shard_index(key.as_bytes()) < SHARD_COUNT);
        }
    }

    #[test]
    fn index_deterministic() {
        assert_eq!(shard_index(b"some-key"), shard_index(b"some-key"));
        assert_eq!(shard_index(b""), shard_index(b""));
    }

    #[test]
    fn index_distribution() {
        // Deterministic spread check: with this many keys every shard should get
        // a healthy share, and none should be wildly over- or under-filled.
        const KEYS: usize = SHARD_COUNT as usize * 200;
        let avg = KEYS / SHARD_COUNT as usize;
        let mut counts = vec![0usize; SHARD_COUNT as usize];
        for i in 0..KEYS {
            counts[shard_index(format!("key-{i}").as_bytes()) as usize] += 1;
        }
        for (shard, &c) in counts.iter().enumerate() {
            assert!(c > avg / 4, "shard {shard} underfilled: {c} (avg {avg})");
            assert!(c < avg * 4, "shard {shard} overfilled: {c} (avg {avg})");
        }
    }

    // Golden vectors pinning the mapping: changing the hash or SHARD_COUNT is a
    // format migration and must break these.
    #[test]
    fn golden_index_vectors() {
        assert_eq!(shard_index(b""), 453);
        assert_eq!(shard_index(b"Hello"), 331);
        assert_eq!(shard_index(b"hello"), 171);
        assert_eq!(shard_index(b"world"), 147);
        assert_eq!(shard_index(&[0, 1, 2, 3, 4]), 1007);
        assert_eq!(shard_index(b"some-key"), 883);
    }

    #[test]
    fn shard_path_format() {
        assert_eq!(shard_path("db/coll", b"Hello"), "db/coll/_s/0331");
    }
}
