//! A thread-safe, byte-weighted LRU cache. Ported from the Go `internal/cache`
//! package.
//!
//! The cache is partitioned into independent shards, each with its own lock and
//! byte budget, to reduce lock contention on this hot DB-level structure.

use std::sync::Mutex;

use glassdb_concurr::shard::{self, Sharded};
use hashlink::LinkedHashMap;

/// Implemented by cached values to report their size in bytes.
pub trait Weighable {
    fn size(&self) -> usize;
}

struct Inner<V> {
    max_size: usize,
    curr_size: usize,
    /// Entries in LRU order: the front is least-recently-used, the back is
    /// most-recently-used. The linked list gives O(1) recency refresh and
    /// eviction, and eviction follows that list order (not the hash buckets),
    /// so behavior is independent of the hasher.
    map: LinkedHashMap<String, V>,
}

struct WeightReplacement {
    size: usize,
    overflowed: bool,
}

fn replace_weight(curr_size: usize, old_size: usize, new_size: usize) -> WeightReplacement {
    let remaining = curr_size
        .checked_sub(old_size)
        .expect("cache weight removal exceeds the accounted size");
    match remaining.checked_add(new_size) {
        Some(size) => WeightReplacement {
            size,
            overflowed: false,
        },
        None => WeightReplacement {
            size: usize::MAX,
            overflowed: true,
        },
    }
}

impl<V: Weighable + Clone> Inner<V> {
    fn delete_entry(&mut self, key: &str) {
        let Some(old_size) = self.map.get(key).map(Weighable::size) else {
            return;
        };
        self.curr_size = replace_weight(self.curr_size, old_size, 0).size;
        self.map.remove(key);
    }

    fn recompute_weight(&mut self) -> bool {
        let mut curr_size = 0;
        let mut overflowed = false;
        for value in self.map.values() {
            let replacement = replace_weight(curr_size, 0, value.size());
            curr_size = replacement.size;
            overflowed = replacement.overflowed;
            if overflowed {
                break;
            }
        }
        self.curr_size = curr_size;
        overflowed
    }

    fn remove_oldest(&mut self, mut overflowed: bool, evicted: &mut Vec<(String, V)>) {
        while overflowed || self.curr_size > self.max_size {
            let Some((k, v)) = self.map.pop_front() else {
                return;
            };
            if overflowed {
                // A saturated total is not exact, so subtracting an eviction
                // could make the cache appear smaller than its survivors.
                overflowed = self.recompute_weight();
            } else {
                let replacement = replace_weight(self.curr_size, v.size(), 0);
                self.curr_size = replacement.size;
                overflowed = replacement.overflowed;
            }
            evicted.push((k, v));
        }
    }
}

/// One independent partition of the cache, holding its own lock, entries map,
/// LRU list, and byte budget.
pub struct CacheShard<V> {
    inner: Mutex<Inner<V>>,
}

impl<V: Weighable + Clone> CacheShard<V> {
    /// Creates a cache shard with the given maximum size in bytes.
    pub fn new(max_size: usize) -> Self {
        CacheShard {
            inner: Mutex::new(Inner {
                max_size,
                curr_size: 0,
                map: LinkedHashMap::new(),
            }),
        }
    }

    fn get(&self, key: &str) -> Option<V> {
        let mut inner = self.inner.lock().unwrap();
        // `to_back` moves the entry to the most-recently-used position and
        // returns it to clone.
        inner.map.to_back(key).cloned()
    }

    #[cfg(test)]
    fn set(&self, key: &str, val: V) {
        self.update(key, |_| Some(val));
    }

    #[cfg(test)]
    fn update<F>(&self, key: &str, f: F)
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.update_with_result(key, |old| (f(old), ()));
    }

    #[cfg(test)]
    fn update_with_result<F, R>(&self, key: &str, f: F) -> R
    where
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        self.update_evicting(key, f).0
    }

    /// Updates `key` and returns the entries that the update evicted for
    /// capacity, including a new value that does not fit.
    fn update_evicting<F, R>(&self, key: &str, f: F) -> (R, Vec<(String, V)>)
    where
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        let mut inner = self.inner.lock().unwrap();
        let mut evicted = Vec::new();
        let old = inner.map.get(key).cloned();
        let old_size = old.as_ref().map_or(0, Weighable::size);
        let (new, result) = f(old);
        match new {
            None => {
                // Remove an existing entry, or leave an absent one absent.
                inner.delete_entry(key);
            }
            Some(newv) => {
                let new_size = newv.size();
                if inner.max_size == 0 || new_size > inner.max_size {
                    // Keeping an old value here could hide a completed mutation.
                    inner.delete_entry(key);
                    evicted.push((key.to_string(), newv));
                    return (result, evicted);
                }
                let replacement = replace_weight(inner.curr_size, old_size, new_size);
                inner.curr_size = replacement.size;
                // `insert` appends at the back (most-recently-used) and, for an
                // existing key, moves it there while replacing the value.
                inner.map.insert(key.to_string(), newv);
                inner.remove_oldest(replacement.overflowed, &mut evicted);
            }
        }
        (result, evicted)
    }

    fn delete(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.delete_entry(key);
    }

    fn size(&self) -> usize {
        self.inner.lock().unwrap().curr_size
    }

    fn entries(&self) -> Vec<(String, V)> {
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

/// Receives each entry that the cache evicts for capacity. The cache calls it
/// after it releases the shard lock.
pub type EvictionHook<V> = Box<dyn Fn(&str, V) + Send + Sync>;

/// A thread-safe LRU cache that evicts the least-recently-used entries once the
/// total size exceeds the configured maximum. It is partitioned into
/// independent shards, each with its own lock and byte budget.
pub struct Cache<V> {
    sh: Sharded<CacheShard<V>>,
    on_evict: Option<EvictionHook<V>>,
}

impl<V: Weighable + Clone> Cache<V> {
    /// Creates a cache with the given maximum size in bytes. The budget is split
    /// evenly across shards to reduce lock contention. Zero disables retention.
    pub fn new(max_size: usize) -> Self {
        Self::build(max_size, None)
    }

    /// Creates a cache like [`Self::new`] that passes each entry it evicts for
    /// capacity to `on_evict`. A new value that does not fit its shard's
    /// budget counts as evicted. Explicit removals and replacements do not.
    pub fn with_eviction(max_size: usize, on_evict: EvictionHook<V>) -> Self {
        Self::build(max_size, Some(on_evict))
    }

    fn build(max_size: usize, on_evict: Option<EvictionHook<V>>) -> Self {
        let per = max_size / shard::count();
        Cache {
            sh: Sharded::new(move |_| CacheShard::new(per)),
            on_evict,
        }
    }

    /// Returns the value for `key`, moving it to the front of the LRU list.
    pub fn get(&self, key: &str) -> Option<V> {
        self.sh.for_key(key.as_bytes()).get(key)
    }

    /// Stores `val` when it fits its shard's budget. Otherwise removes `key`.
    pub fn set(&self, key: &str, val: V) {
        self.update_with_result(key, |_| (Some(val), ()));
    }

    /// Updates the value under `key` while holding the lock. The closure
    /// receives the old value (or `None`) and returns the new value, or `None`
    /// to remove the entry. A new value that exceeds its shard's budget also
    /// removes the entry.
    pub fn update<F>(&self, key: &str, f: F)
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.update_with_result(key, |old| (f(old), ()));
    }

    /// Removes the entry for `key`.
    pub fn delete(&self, key: &str) {
        self.sh.for_key(key.as_bytes()).delete(key);
    }

    /// Returns the current total size of the cache in bytes across all shards.
    pub fn size(&self) -> usize {
        let mut total = 0;
        self.sh.each(|s| total += s.size());
        total
    }

    /// Returns a copy of every entry. Each shard is copied under its own lock,
    /// so the result is not one atomic snapshot of the whole cache.
    pub fn entries(&self) -> Vec<(String, V)> {
        let mut entries = Vec::new();
        self.sh.each(|s| entries.extend(s.entries()));
        entries
    }

    /// Atomically updates `key` and returns an auxiliary result produced by the
    /// update closure.
    pub(crate) fn update_with_result<F, R>(&self, key: &str, f: F) -> R
    where
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        let (result, evicted) = self.sh.for_key(key.as_bytes()).update_evicting(key, f);
        if let Some(on_evict) = &self.on_evict {
            for (key, value) in evicted {
                on_evict(&key, value);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Debug)]
    struct TestEntry(String);

    impl Weighable for TestEntry {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    #[derive(Clone, Copy, PartialEq, Debug)]
    struct WeightedEntry(usize);

    impl Weighable for WeightedEntry {
        fn size(&self) -> usize {
            self.0
        }
    }

    fn e(s: &str) -> TestEntry {
        TestEntry(s.to_string())
    }

    // The behavior tests drive a single shard directly so they are independent
    // of the host's shard count.
    #[test]
    fn get_set() {
        let c = CacheShard::new(100);
        assert_eq!(c.size(), 0);
        c.set("a", e("foo"));
        assert_eq!(c.get("a"), Some(e("foo")));
        assert_eq!(c.size(), 3);
        c.set("a", e("barbaz"));
        assert_eq!(c.get("a"), Some(e("barbaz")));
        assert_eq!(c.size(), 6);
    }

    #[test]
    fn delete() {
        let c = CacheShard::new(100);
        c.set("k1", e("k1"));
        c.set("k2", e("k2"));
        assert_eq!(c.size(), 4);
        c.delete("k1");
        assert_eq!(c.get("k1"), None);
        assert!(c.get("k2").is_some());
        assert_eq!(c.size(), 2);
    }

    #[test]
    fn update_existing() {
        let c = CacheShard::new(100);
        c.set("a", e("foo"));
        assert_eq!(c.size(), 3);
        c.update("a", |_| Some(e("barbaz")));
        assert_eq!(c.get("a"), Some(e("barbaz")));
        assert_eq!(c.size(), 6);
        c.update("a", |_| Some(e("x")));
        assert_eq!(c.get("a"), Some(e("x")));
        assert_eq!(c.size(), 1);
    }

    #[test]
    fn update_new() {
        let c = CacheShard::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            Some(e("bar"))
        });
        assert_eq!(c.get("a"), Some(e("bar")));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn update_delete() {
        let c = CacheShard::new(100);
        c.set("a", e("foo"));
        c.update("a", |_| None);
        assert_eq!(c.get("a"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn update_nope() {
        let c: CacheShard<TestEntry> = CacheShard::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            None
        });
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn update_with_result_updates_and_returns() {
        let c = CacheShard::new(100);
        c.set("a", e("before"));

        let previous = c.update_with_result("a", |old| (Some(e("after")), old));

        assert_eq!(previous, Some(e("before")));
        assert_eq!(c.get("a"), Some(e("after")));
    }

    #[test]
    fn evicts_lru() {
        // Budget for two 3-byte entries.
        let c = CacheShard::new(6);
        c.set("a", e("aaa"));
        c.set("b", e("bbb"));
        assert_eq!(c.size(), 6);
        // Adding a third entry evicts the least recently used ("a").
        c.set("c", e("ccc"));
        assert_eq!(c.get("a"), None);
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
        assert_eq!(c.size(), 6);
    }

    #[test]
    fn rejects_an_entry_larger_than_the_budget() {
        let c = CacheShard::new(2);
        c.set("a", e("aaaa"));
        assert_eq!(c.get("a"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn oversized_entry_does_not_evict_other_keys() {
        let c = CacheShard::new(3);
        c.set("kept", e("fit"));
        c.set("large", e("large"));
        assert_eq!(c.get("kept"), Some(e("fit")));
        assert_eq!(c.get("large"), None);
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn oversized_update_removes_old_value_and_returns_its_result() {
        let c = CacheShard::new(3);
        c.set("a", e("old"));
        let result = c.update_with_result("a", |_| (Some(e("large")), 42));
        assert_eq!(result, 42);
        assert_eq!(c.get("a"), None);
        assert_eq!(c.size(), 0);
        c.set("a", e("new"));
        assert_eq!(c.get("a"), Some(e("new")));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn zero_budget_retains_no_entries_even_with_zero_weight() {
        let c = Cache::new(0);
        c.set("a", e(""));
        c.update("b", |_| Some(e("value")));
        assert_eq!(c.get("a"), None);
        assert_eq!(c.get("b"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn accounts_for_usize_max_weight() {
        let c = CacheShard::new(usize::MAX);

        c.set("a", WeightedEntry(usize::MAX));
        assert_eq!(c.get("a"), Some(WeightedEntry(usize::MAX)));
        assert_eq!(c.size(), usize::MAX);

        c.set("a", WeightedEntry(1));
        assert_eq!(c.size(), 1);
        c.delete("a");
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn multi_entry_overflow_recomputes_remaining_weight() {
        let c = CacheShard::new(usize::MAX);
        c.set("oldest", WeightedEntry(usize::MAX - 2));
        c.set("middle", WeightedEntry(1));

        c.set("newest", WeightedEntry(2));

        assert_eq!(c.get("oldest"), None);
        assert_eq!(c.get("middle"), Some(WeightedEntry(1)));
        assert_eq!(c.get("newest"), Some(WeightedEntry(2)));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn overflow_keeps_evicting_until_remaining_weight_fits() {
        let c = CacheShard::new(usize::MAX);
        c.set("oldest", WeightedEntry(1));
        c.set("middle", WeightedEntry(usize::MAX - 1));

        c.set("newest", WeightedEntry(2));

        assert_eq!(c.get("oldest"), None);
        assert_eq!(c.get("middle"), None);
        assert_eq!(c.get("newest"), Some(WeightedEntry(2)));
        assert_eq!(c.size(), 2);
    }

    #[test]
    #[should_panic(expected = "cache weight removal exceeds the accounted size")]
    fn weight_removal_underflow_panics() {
        replace_weight(0, 1, 0);
    }

    // Returns `count` distinct keys that hash to the given shard.
    fn keys_for_shard(target: usize, n: usize, count: usize) -> Vec<String> {
        let mut res = Vec::new();
        let mut i = 0;
        while res.len() < count {
            let k = format!("k{i}");
            if shard::index(k.as_bytes(), n) == target {
                res.push(k);
            }
            i += 1;
        }
        res
    }

    #[test]
    fn sharded() {
        let n = shard::count();
        if n < 2 {
            return; // sharded behavior requires parallelism >= 2
        }
        // Per-shard budget of exactly 6 bytes.
        let c = Cache::new(6 * n);

        // Two distinct keys in shard 0 and one in shard 1.
        let s0 = keys_for_shard(0, n, 2);
        let s1 = keys_for_shard(1, n, 1);

        // Routing across shards and size summation.
        c.set(&s0[0], e("aaa")); // 3 bytes in shard 0
        c.set(&s1[0], e("bbb")); // 3 bytes in shard 1
        assert_eq!(c.size(), 6);

        // Overflowing shard 0 only evicts within shard 0; shard 1 is untouched.
        c.set(&s0[1], e("cccc")); // 4 bytes pushes shard 0 to 7 > 6
        assert_eq!(
            c.get(&s0[0]),
            None,
            "least recently used entry in shard 0 should be evicted"
        );
        assert!(c.get(&s0[1]).is_some());
        assert!(
            c.get(&s1[0]).is_some(),
            "entry in shard 1 must be unaffected by shard 0 eviction"
        );
    }

    #[test]
    fn eviction_hook_receives_capacity_evictions_only() {
        let n = shard::count();
        let evicted = std::sync::Arc::new(Mutex::new(Vec::new()));
        let sink = evicted.clone();
        let c = Cache::with_eviction(
            6 * n,
            Box::new(move |key: &str, value: TestEntry| {
                sink.lock().unwrap().push((key.to_owned(), value));
            }),
        );
        let keys = keys_for_shard(0, n, 4);
        c.set(&keys[0], e("aaa"));
        c.set(&keys[1], e("bbb"));
        c.delete(&keys[1]);
        c.set(&keys[1], e("bbb"));
        c.update(&keys[1], |_| Some(e("ccc")));
        assert!(evicted.lock().unwrap().is_empty());

        c.set(&keys[2], e("ddd"));
        c.set(&keys[3], e("too large"));
        assert_eq!(
            *evicted.lock().unwrap(),
            vec![
                (keys[0].clone(), e("aaa")),
                (keys[3].clone(), e("too large"))
            ]
        );
        let mut entries = c.entries();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = vec![(keys[1].clone(), e("ccc")), (keys[2].clone(), e("ddd"))];
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(entries, expected);
    }
}
