//! A thread-safe, byte-weighted LRU cache. Ported from the Go `internal/cache`
//! package.
//!
//! The cache is partitioned into independent shards, each with its own lock and
//! byte budget, to reduce lock contention on this hot DB-level structure.

use std::collections::HashMap;
use std::sync::Mutex;

use glassdb_concurr::shard::{self, Sharded};

/// Implemented by cached values to report their size in bytes.
pub trait Weighable {
    fn size_b(&self) -> usize;
}

struct Inner<V> {
    max_size_b: usize,
    curr_size_b: usize,
    map: HashMap<String, V>,
    /// Most-recently-used at index 0, least-recently-used at the end.
    order: Vec<String>,
}

impl<V: Weighable + Clone> Inner<V> {
    fn move_to_front(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key)
            && pos != 0
        {
            let k = self.order.remove(pos);
            self.order.insert(0, k);
        }
    }

    fn delete_entry(&mut self, key: &str) {
        if let Some(v) = self.map.remove(key) {
            self.curr_size_b = self.curr_size_b.saturating_sub(v.size_b());
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
        }
    }

    fn remove_oldest(&mut self) {
        while self.curr_size_b > self.max_size_b {
            // Never evict the most-recently-used entry, even if it alone
            // exceeds the shard budget. Otherwise a freshly written value
            // (e.g. one larger than max_size_b/shards) would be dropped
            // immediately, defeating the write and breaking callers that read
            // back their own writes. Overshoot is bounded to one entry per
            // shard.
            if self.order.len() <= 1 {
                return;
            }
            let Some(key) = self.order.pop() else {
                return;
            };
            if let Some(v) = self.map.remove(&key) {
                self.curr_size_b = self.curr_size_b.saturating_sub(v.size_b());
            }
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
    pub fn new(max_size_b: usize) -> Self {
        CacheShard {
            inner: Mutex::new(Inner {
                max_size_b,
                curr_size_b: 0,
                map: HashMap::new(),
                order: Vec::new(),
            }),
        }
    }

    fn get(&self, key: &str) -> Option<V> {
        let mut inner = self.inner.lock().unwrap();
        if inner.map.contains_key(key) {
            inner.move_to_front(key);
            inner.map.get(key).cloned()
        } else {
            None
        }
    }

    fn set(&self, key: &str, val: V) {
        self.update(key, |_| Some(val));
    }

    fn update<F>(&self, key: &str, f: F)
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let mut inner = self.inner.lock().unwrap();
        if inner.map.contains_key(key) {
            inner.move_to_front(key);
            let old = inner.map.get(key).cloned().unwrap();
            let old_size = old.size_b();
            match f(Some(old)) {
                None => {
                    inner.delete_entry(key);
                    return;
                }
                Some(newv) => {
                    let new_size = newv.size_b();
                    inner.curr_size_b =
                        (inner.curr_size_b as i64 + new_size as i64 - old_size as i64) as usize;
                    inner.map.insert(key.to_string(), newv);
                }
            }
        } else {
            match f(None) {
                None => return,
                Some(newv) => {
                    inner.curr_size_b += newv.size_b();
                    inner.order.insert(0, key.to_string());
                    inner.map.insert(key.to_string(), newv);
                }
            }
        }
        inner.remove_oldest();
    }

    fn delete(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.delete_entry(key);
    }

    fn size_b(&self) -> usize {
        self.inner.lock().unwrap().curr_size_b
    }
}

/// A thread-safe LRU cache that evicts the least-recently-used entries once the
/// total size exceeds the configured maximum. It is partitioned into
/// independent shards, each with its own lock and byte budget.
pub struct Cache<V> {
    sh: Sharded<CacheShard<V>>,
}

impl<V: Weighable + Clone> Cache<V> {
    /// Creates a cache with the given maximum size in bytes. The budget is split
    /// evenly across shards to reduce lock contention.
    pub fn new(max_size_b: usize) -> Self {
        let per = max_size_b / shard::count();
        Cache {
            sh: Sharded::new(move |_| CacheShard::new(per)),
        }
    }

    /// Returns the value for `key`, moving it to the front of the LRU list.
    pub fn get(&self, key: &str) -> Option<V> {
        self.sh.for_key(key.as_bytes()).get(key)
    }

    /// Stores `val` under `key`.
    pub fn set(&self, key: &str, val: V) {
        self.sh.for_key(key.as_bytes()).set(key, val);
    }

    /// Updates the value under `key` while holding the lock. The closure
    /// receives the old value (or `None`) and returns the new value, or `None`
    /// to remove the entry.
    pub fn update<F>(&self, key: &str, f: F)
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.sh.for_key(key.as_bytes()).update(key, f);
    }

    /// Removes the entry for `key`.
    pub fn delete(&self, key: &str) {
        self.sh.for_key(key.as_bytes()).delete(key);
    }

    /// Returns the current total size of the cache in bytes across all shards.
    pub fn size_b(&self) -> usize {
        let mut total = 0;
        self.sh.each(|s| total += s.size_b());
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, Debug)]
    struct TestEntry(String);

    impl Weighable for TestEntry {
        fn size_b(&self) -> usize {
            self.0.len()
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
        assert_eq!(c.size_b(), 0);
        c.set("a", e("foo"));
        assert_eq!(c.get("a"), Some(e("foo")));
        assert_eq!(c.size_b(), 3);
        c.set("a", e("barbaz"));
        assert_eq!(c.get("a"), Some(e("barbaz")));
        assert_eq!(c.size_b(), 6);
    }

    #[test]
    fn delete() {
        let c = CacheShard::new(100);
        c.set("k1", e("k1"));
        c.set("k2", e("k2"));
        assert_eq!(c.size_b(), 4);
        c.delete("k1");
        assert_eq!(c.get("k1"), None);
        assert!(c.get("k2").is_some());
        assert_eq!(c.size_b(), 2);
    }

    #[test]
    fn update_existing() {
        let c = CacheShard::new(100);
        c.set("a", e("foo"));
        assert_eq!(c.size_b(), 3);
        c.update("a", |_| Some(e("barbaz")));
        assert_eq!(c.get("a"), Some(e("barbaz")));
        assert_eq!(c.size_b(), 6);
    }

    #[test]
    fn update_new() {
        let c = CacheShard::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            Some(e("bar"))
        });
        assert_eq!(c.get("a"), Some(e("bar")));
        assert_eq!(c.size_b(), 3);
    }

    #[test]
    fn update_delete() {
        let c = CacheShard::new(100);
        c.set("a", e("foo"));
        c.update("a", |_| None);
        assert_eq!(c.get("a"), None);
        assert_eq!(c.size_b(), 0);
    }

    #[test]
    fn update_nope() {
        let c: CacheShard<TestEntry> = CacheShard::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            None
        });
        assert_eq!(c.size_b(), 0);
    }

    #[test]
    fn evicts_lru() {
        // Budget for two 3-byte entries.
        let c = CacheShard::new(6);
        c.set("a", e("aaa"));
        c.set("b", e("bbb"));
        assert_eq!(c.size_b(), 6);
        // Adding a third entry evicts the least recently used ("a").
        c.set("c", e("ccc"));
        assert_eq!(c.get("a"), None);
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
        assert_eq!(c.size_b(), 6);
    }

    #[test]
    fn never_evicts_sole_entry() {
        // A single entry larger than the budget is kept (bounded overshoot).
        let c = CacheShard::new(2);
        c.set("a", e("aaaa"));
        assert_eq!(c.get("a"), Some(e("aaaa")));
        assert_eq!(c.size_b(), 4);
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

        // Routing across shards and size_b summation.
        c.set(&s0[0], e("aaa")); // 3 bytes in shard 0
        c.set(&s1[0], e("bbb")); // 3 bytes in shard 1
        assert_eq!(c.size_b(), 6);

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
}
