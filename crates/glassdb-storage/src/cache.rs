//! A thread-safe, byte-weighted LRU cache. Ported from the Go `internal/cache`
//! package.

use std::collections::HashMap;
use std::sync::Mutex;

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
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            if pos != 0 {
                let k = self.order.remove(pos);
                self.order.insert(0, k);
            }
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
            let Some(key) = self.order.pop() else {
                return;
            };
            if let Some(v) = self.map.remove(&key) {
                self.curr_size_b = self.curr_size_b.saturating_sub(v.size_b());
            }
        }
    }
}

/// A thread-safe LRU cache that evicts the least-recently-used entries once the
/// total size exceeds the configured maximum.
pub struct Cache<V> {
    inner: Mutex<Inner<V>>,
}

impl<V: Weighable + Clone> Cache<V> {
    /// Creates a cache with the given maximum size in bytes.
    pub fn new(max_size_b: usize) -> Self {
        Cache {
            inner: Mutex::new(Inner {
                max_size_b,
                curr_size_b: 0,
                map: HashMap::new(),
                order: Vec::new(),
            }),
        }
    }

    /// Returns the value for `key`, moving it to the front of the LRU list.
    pub fn get(&self, key: &str) -> Option<V> {
        let mut inner = self.inner.lock().unwrap();
        if inner.map.contains_key(key) {
            inner.move_to_front(key);
            inner.map.get(key).cloned()
        } else {
            None
        }
    }

    /// Stores `val` under `key`.
    pub fn set(&self, key: &str, val: V) {
        self.update(key, |_| Some(val));
    }

    /// Updates the value under `key` while holding the lock. The closure
    /// receives the old value (or `None`) and returns the new value, or `None`
    /// to remove the entry.
    pub fn update<F>(&self, key: &str, f: F)
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

    /// Removes the entry for `key`.
    pub fn delete(&self, key: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.delete_entry(key);
    }

    /// Returns the current total size of the cache in bytes.
    pub fn size_b(&self) -> usize {
        self.inner.lock().unwrap().curr_size_b
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

    #[test]
    fn get_set() {
        let c = Cache::new(100);
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
        let c = Cache::new(100);
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
        let c = Cache::new(100);
        c.set("a", e("foo"));
        assert_eq!(c.size_b(), 3);
        c.update("a", |_| Some(e("barbaz")));
        assert_eq!(c.get("a"), Some(e("barbaz")));
        assert_eq!(c.size_b(), 6);
    }

    #[test]
    fn update_new() {
        let c = Cache::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            Some(e("bar"))
        });
        assert_eq!(c.get("a"), Some(e("bar")));
        assert_eq!(c.size_b(), 3);
    }

    #[test]
    fn update_delete() {
        let c = Cache::new(100);
        c.set("a", e("foo"));
        c.update("a", |_| None);
        assert_eq!(c.get("a"), None);
        assert_eq!(c.size_b(), 0);
    }

    #[test]
    fn update_nope() {
        let c: Cache<TestEntry> = Cache::new(100);
        c.update("a", |old| {
            assert!(old.is_none());
            None
        });
        assert_eq!(c.size_b(), 0);
    }

    #[test]
    fn evicts_lru() {
        let c = Cache::new(6);
        c.set("a", e("aaa"));
        c.set("b", e("bbb"));
        // Touch a so b is the LRU.
        assert!(c.get("a").is_some());
        c.set("d", e("ddd")); // exceeds capacity, evicts b.
        assert_eq!(c.get("b"), None);
        assert!(c.get("a").is_some());
        assert!(c.get("d").is_some());
    }
}
