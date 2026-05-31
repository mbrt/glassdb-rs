//! Partitions a shared structure into independent shards selected by key hash,
//! reducing lock contention on hot DB-level maps. Ported from the Go
//! `internal/shard` package.

use std::thread::available_parallelism;

const FNV_OFFSET_32: u32 = 2166136261;
const FNV_PRIME_32: u32 = 16777619;

/// Returns the recommended number of shards: the next power of two greater than
/// or equal to the available parallelism (the Rust analog of `GOMAXPROCS`).
pub fn count() -> usize {
    let par = available_parallelism().map(|n| n.get()).unwrap_or(1);
    next_pow2(par)
}

/// Returns the shard index for `key` given `n` shards. `n` must be a power of
/// two, so the modulo reduces to a bit mask.
pub fn index(key: &[u8], n: usize) -> usize {
    (fnv1a(key) & (n as u32 - 1)) as usize
}

/// Owns [`count`] independent shards of type `T`, routed by key hash. It is
/// meant to be embedded in a wrapper that delegates per-key operations to the
/// shard returned by [`Sharded::for_key`].
pub struct Sharded<T> {
    shards: Vec<T>,
}

impl<T> Sharded<T> {
    /// Builds [`count`] shards, initializing shard `i` with `new_shard(i)`.
    pub fn new<F>(new_shard: F) -> Self
    where
        F: Fn(usize) -> T,
    {
        let n = count();
        let shards = (0..n).map(new_shard).collect();
        Sharded { shards }
    }

    /// Returns the shard responsible for `key`.
    pub fn for_key(&self, key: &[u8]) -> &T {
        &self.shards[index(key, self.shards.len())]
    }

    /// Returns the number of shards.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Reports whether there are no shards (never true in practice).
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }

    /// Calls `f` once for every shard. The order is unspecified.
    pub fn each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        for s in &self.shards {
            f(s);
        }
    }
}

/// Inline FNV-1a 32-bit hash that avoids allocating a hasher.
fn fnv1a(key: &[u8]) -> u32 {
    let mut h = FNV_OFFSET_32;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(FNV_PRIME_32);
    }
    h
}

/// Returns the smallest power of two greater than or equal to `n`, returning 1
/// for `n <= 1`.
fn next_pow2(n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    let mut p = 1;
    while p < n {
        p <<= 1;
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_is_power_of_two() {
        let n = count();
        assert!(n >= 1);
        assert_eq!(n & (n - 1), 0, "count() must be a power of two, got {n}");
    }

    #[test]
    fn next_pow2_table() {
        let cases = [(0, 1), (1, 1), (2, 2), (3, 4), (5, 8), (8, 8), (9, 16)];
        for (input, want) in cases {
            assert_eq!(next_pow2(input), want, "next_pow2({input})");
        }
    }

    #[test]
    fn index_in_range() {
        const N: usize = 8;
        for i in 0..1000 {
            let key = format!("key-{i}");
            let idx = index(key.as_bytes(), N);
            assert!(idx < N);
        }
    }

    #[test]
    fn index_deterministic() {
        const N: usize = 16;
        assert_eq!(index(b"some-key", N), index(b"some-key", N));
    }

    #[test]
    fn index_distribution() {
        const N: usize = 8;
        const KEYS: usize = 8000;
        let mut counts = [0usize; N];
        for i in 0..KEYS {
            counts[index(format!("key-{i}").as_bytes(), N)] += 1;
        }
        // Every shard should get a non-trivial share of the keys.
        for (shard, &c) in counts.iter().enumerate() {
            assert!(c > KEYS / N / 2, "shard {shard} underfilled: {c}");
        }
    }

    #[test]
    fn sharded_for_and_each() {
        let s = Sharded::new(|i| i);
        assert_eq!(s.len(), count());

        // for_key is stable for a given key.
        let first = *s.for_key(b"a-key");
        assert_eq!(first, *s.for_key(b"a-key"));

        // each visits every shard exactly once.
        let mut visited = vec![0usize; s.len()];
        s.each(|&id| visited[id] += 1);
        for (id, &v) in visited.iter().enumerate() {
            assert_eq!(v, 1, "shard {id} not visited exactly once");
        }
    }
}
