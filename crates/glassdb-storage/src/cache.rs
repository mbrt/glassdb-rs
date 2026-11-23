//! A thread-safe, byte-weighted LRU cache. Ported from the Go `internal/cache`
//! package.
//!
//! The cache is partitioned into independent shards, each with its own lock and
//! byte budget, to reduce lock contention on this hot DB-level structure.
//!
//! Each path has two slots: its latest state and its earlier states (ADR-103).
//! The two slots of a path are separate entries in one LRU order, and they are
//! always in the same shard.

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::Mutex;

use glassdb_concurr::shard::{self, Sharded};
use hashlink::LinkedHashMap;
use hashlink::linked_hash_map::RawEntryMut;

/// Implemented by cached values to report their size in bytes.
pub trait Weighable {
    fn size(&self) -> usize;
}

/// The slot of a path that an entry fills.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Slot {
    /// The latest known state of the path.
    Latest,
    /// The earlier states of the path that the stale views can use.
    Earlier,
}

/// The key of one entry. It hashes as `(slot, path)`, so a lookup can hash a
/// borrowed path and compare it without an allocation.
#[derive(PartialEq, Eq)]
struct Key {
    slot: Slot,
    path: String,
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.slot, self.path.as_str()).hash(state);
    }
}

impl Key {
    fn is(&self, slot: Slot, path: &str) -> bool {
        self.slot == slot && self.path == path
    }
}

/// An entry that the cache evicted for capacity.
type Evicted<V> = (Slot, String, V);

struct Inner<V> {
    max_size: usize,
    curr_size: usize,
    /// Entries in LRU order: the front is least-recently-used, the back is
    /// most-recently-used. The linked list gives O(1) recency refresh and
    /// eviction, and eviction follows that list order (not the hash buckets),
    /// so behavior is independent of the hasher.
    map: LinkedHashMap<Key, V>,
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
    fn hash(&self, slot: Slot, path: &str) -> u64 {
        self.map.hasher().hash_one((slot, path))
    }

    fn peek(&self, slot: Slot, path: &str) -> Option<&V> {
        let hash = self.hash(slot, path);
        self.map
            .raw_entry()
            .from_hash(hash, |key| key.is(slot, path))
            .map(|(_, value)| value)
    }

    /// Moves the entry to the most-recently-used position and returns it.
    fn refresh(&mut self, slot: Slot, path: &str) -> Option<&V> {
        let hash = self.hash(slot, path);
        match self
            .map
            .raw_entry_mut()
            .from_hash(hash, |key| key.is(slot, path))
        {
            RawEntryMut::Occupied(mut entry) => {
                entry.to_back();
                Some(entry.into_mut())
            }
            RawEntryMut::Vacant(_) => None,
        }
    }

    fn delete_entry(&mut self, slot: Slot, path: &str) {
        let hash = self.hash(slot, path);
        if let RawEntryMut::Occupied(entry) = self
            .map
            .raw_entry_mut()
            .from_hash(hash, |key| key.is(slot, path))
        {
            let old = entry.remove();
            self.curr_size = replace_weight(self.curr_size, old.size(), 0).size;
        }
    }

    /// Stores `new` in a slot, or removes the slot when `new` is `None` or
    /// does not fit the budget. Returns whether the total weight overflowed.
    /// The caller evicts for capacity after it stores every slot.
    fn put(
        &mut self,
        slot: Slot,
        path: &str,
        old_size: usize,
        new: Option<V>,
        evicted: &mut Vec<Evicted<V>>,
    ) -> bool {
        let Some(new) = new else {
            // Remove an existing entry, or leave an absent one absent.
            self.delete_entry(slot, path);
            return false;
        };
        let new_size = new.size();
        if self.max_size == 0 || new_size > self.max_size {
            // Keeping an old value here could hide a completed mutation.
            self.delete_entry(slot, path);
            evicted.push((slot, path.to_string(), new));
            return false;
        }
        let replacement = replace_weight(self.curr_size, old_size, new_size);
        self.curr_size = replacement.size;
        // `insert` appends at the back (most-recently-used) and, for an
        // existing key, moves it there while replacing the value.
        self.map.insert(
            Key {
                slot,
                path: path.to_string(),
            },
            new,
        );
        replacement.overflowed
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

    fn remove_oldest(&mut self, mut overflowed: bool, evicted: &mut Vec<Evicted<V>>) {
        while overflowed || self.curr_size > self.max_size {
            let Some((key, v)) = self.map.pop_front() else {
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
            evicted.push((key.slot, key.path, v));
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

    fn get(&self, slot: Slot, path: &str) -> Option<V> {
        self.inner.lock().unwrap().refresh(slot, path).cloned()
    }

    /// Updates one slot and returns the entries that the update evicted for
    /// capacity, including a new value that does not fit.
    fn update_evicting<F, R>(&self, slot: Slot, path: &str, f: F) -> (R, Vec<Evicted<V>>)
    where
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        let mut inner = self.inner.lock().unwrap();
        let mut evicted = Vec::new();
        let old = inner.peek(slot, path).cloned();
        let old_size = old.as_ref().map_or(0, Weighable::size);
        let (new, result) = f(old);
        let overflowed = inner.put(slot, path, old_size, new, &mut evicted);
        inner.remove_oldest(overflowed, &mut evicted);
        (result, evicted)
    }

    /// Updates both slots of `path` under one lock, and returns the entries
    /// that the update evicted for capacity.
    fn update_both_evicting<F, R>(&self, path: &str, f: F) -> (R, Vec<Evicted<V>>)
    where
        F: FnOnce(Option<V>, Option<V>) -> (Option<V>, Option<V>, R),
    {
        let mut inner = self.inner.lock().unwrap();
        let mut evicted = Vec::new();
        let latest = inner.peek(Slot::Latest, path).cloned();
        let earlier = inner.peek(Slot::Earlier, path).cloned();
        let latest_size = latest.as_ref().map_or(0, Weighable::size);
        let earlier_size = earlier.as_ref().map_or(0, Weighable::size);
        let (latest, earlier, result) = f(latest, earlier);
        // The earlier slot goes first, so that the latest state is the most
        // recently used of the two.
        let mut overflowed = earlier.is_some()
            && inner.put(Slot::Earlier, path, earlier_size, earlier, &mut evicted);
        overflowed |= inner.put(Slot::Latest, path, latest_size, latest, &mut evicted);
        if overflowed {
            overflowed = inner.recompute_weight();
        }
        inner.remove_oldest(overflowed, &mut evicted);
        (result, evicted)
    }

    fn delete(&self, slot: Slot, path: &str) {
        self.inner.lock().unwrap().delete_entry(slot, path);
    }

    fn size(&self) -> usize {
        self.inner.lock().unwrap().curr_size
    }

    fn entries(&self) -> Vec<(Slot, String, V)> {
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .iter()
            .map(|(key, value)| (key.slot, key.path.clone(), value.clone()))
            .collect()
    }
}

/// Receives each entry that the cache evicts for capacity. The cache calls it
/// after it releases the shard lock.
pub type EvictionHook<V> = Box<dyn Fn(Slot, &str, V) + Send + Sync>;

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

    /// The shard of both slots of `path`.
    fn shard(&self, path: &str) -> &CacheShard<V> {
        self.sh.for_key(path.as_bytes())
    }

    /// Returns the value in a slot of `path`, and moves it to the
    /// most-recently-used position.
    pub fn get(&self, slot: Slot, path: &str) -> Option<V> {
        self.shard(path).get(slot, path)
    }

    /// Updates a slot of `path` while holding the lock. The closure receives
    /// the old value (or `None`) and returns the new value, or `None` to remove
    /// the entry. A new value that exceeds its shard's budget also removes the
    /// entry.
    pub fn update<F>(&self, slot: Slot, path: &str, f: F)
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.update_with_result(slot, path, |old| (f(old), ()));
    }

    /// Removes a slot of `path`.
    pub fn delete(&self, slot: Slot, path: &str) {
        self.shard(path).delete(slot, path);
    }

    /// Returns the current total size of the cache in bytes across all shards.
    pub fn size(&self) -> usize {
        let mut total = 0;
        self.sh.each(|s| total += s.size());
        total
    }

    /// Returns a copy of every entry. Each shard is copied under its own lock,
    /// so the result is not one atomic snapshot of the whole cache.
    pub fn entries(&self) -> Vec<(Slot, String, V)> {
        let mut entries = Vec::new();
        self.sh.each(|s| entries.extend(s.entries()));
        entries
    }

    /// Atomically updates a slot of `path` and returns an auxiliary result
    /// produced by the update closure.
    pub(crate) fn update_with_result<F, R>(&self, slot: Slot, path: &str, f: F) -> R
    where
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        let (result, evicted) = self.shard(path).update_evicting(slot, path, f);
        self.evicted(evicted);
        result
    }

    /// Atomically updates both slots of `path`. The closure receives the old
    /// latest and earlier values. It returns the new latest value, the new
    /// earlier value or `None` to leave the earlier slot as it is, and an
    /// auxiliary result.
    pub(crate) fn update_both<F, R>(&self, path: &str, f: F) -> R
    where
        F: FnOnce(Option<V>, Option<V>) -> (Option<V>, Option<V>, R),
    {
        let (result, evicted) = self.shard(path).update_both_evicting(path, f);
        self.evicted(evicted);
        result
    }

    fn evicted(&self, evicted: Vec<Evicted<V>>) {
        if let Some(on_evict) = &self.on_evict {
            for (slot, path, value) in evicted {
                on_evict(slot, &path, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Slot = Slot::Latest;

    /// Shorthands for one-slot updates in the tests.
    trait TestUpdates<V> {
        fn update_with_result<R>(
            &self,
            slot: Slot,
            path: &str,
            f: impl FnOnce(Option<V>) -> (Option<V>, R),
        ) -> R;

        fn update(&self, slot: Slot, path: &str, f: impl FnOnce(Option<V>) -> Option<V>) {
            self.update_with_result(slot, path, |old| (f(old), ()));
        }

        fn set(&self, slot: Slot, path: &str, val: V) {
            self.update(slot, path, |_| Some(val));
        }
    }

    impl<V: Weighable + Clone> TestUpdates<V> for CacheShard<V> {
        fn update_with_result<R>(
            &self,
            slot: Slot,
            path: &str,
            f: impl FnOnce(Option<V>) -> (Option<V>, R),
        ) -> R {
            self.update_evicting(slot, path, f).0
        }
    }

    impl<V: Weighable + Clone> TestUpdates<V> for Cache<V> {
        fn update_with_result<R>(
            &self,
            slot: Slot,
            path: &str,
            f: impl FnOnce(Option<V>) -> (Option<V>, R),
        ) -> R {
            Cache::update_with_result(self, slot, path, f)
        }
    }

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
        c.set(L, "a", e("foo"));
        assert_eq!(c.get(L, "a"), Some(e("foo")));
        assert_eq!(c.size(), 3);
        c.set(L, "a", e("barbaz"));
        assert_eq!(c.get(L, "a"), Some(e("barbaz")));
        assert_eq!(c.size(), 6);
    }

    #[test]
    fn delete() {
        let c = CacheShard::new(100);
        c.set(L, "k1", e("k1"));
        c.set(L, "k2", e("k2"));
        assert_eq!(c.size(), 4);
        c.delete(L, "k1");
        assert_eq!(c.get(L, "k1"), None);
        assert!(c.get(L, "k2").is_some());
        assert_eq!(c.size(), 2);
    }

    #[test]
    fn update_existing() {
        let c = CacheShard::new(100);
        c.set(L, "a", e("foo"));
        assert_eq!(c.size(), 3);
        c.update(L, "a", |_| Some(e("barbaz")));
        assert_eq!(c.get(L, "a"), Some(e("barbaz")));
        assert_eq!(c.size(), 6);
        c.update(L, "a", |_| Some(e("x")));
        assert_eq!(c.get(L, "a"), Some(e("x")));
        assert_eq!(c.size(), 1);
    }

    #[test]
    fn update_new() {
        let c = CacheShard::new(100);
        c.update(L, "a", |old| {
            assert!(old.is_none());
            Some(e("bar"))
        });
        assert_eq!(c.get(L, "a"), Some(e("bar")));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn update_delete() {
        let c = CacheShard::new(100);
        c.set(L, "a", e("foo"));
        c.update(L, "a", |_| None);
        assert_eq!(c.get(L, "a"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn update_nope() {
        let c: CacheShard<TestEntry> = CacheShard::new(100);
        c.update(L, "a", |old| {
            assert!(old.is_none());
            None
        });
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn update_with_result_updates_and_returns() {
        let c = CacheShard::new(100);
        c.set(L, "a", e("before"));

        let previous = c.update_with_result(L, "a", |old| (Some(e("after")), old));

        assert_eq!(previous, Some(e("before")));
        assert_eq!(c.get(L, "a"), Some(e("after")));
    }

    #[test]
    fn evicts_lru() {
        // Budget for two 3-byte entries.
        let c = CacheShard::new(6);
        c.set(L, "a", e("aaa"));
        c.set(L, "b", e("bbb"));
        assert_eq!(c.size(), 6);
        // Adding a third entry evicts the least recently used ("a").
        c.set(L, "c", e("ccc"));
        assert_eq!(c.get(L, "a"), None);
        assert!(c.get(L, "b").is_some());
        assert!(c.get(L, "c").is_some());
        assert_eq!(c.size(), 6);
    }

    #[test]
    fn the_two_slots_of_a_path_are_separate_entries() {
        let c = CacheShard::new(100);
        c.set(Slot::Latest, "p", e("new"));
        c.set(Slot::Earlier, "p", e("older"));
        assert_eq!(c.get(Slot::Latest, "p"), Some(e("new")));
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("older")));
        assert_eq!(c.size(), 8);
        c.delete(Slot::Latest, "p");
        assert_eq!(c.get(Slot::Latest, "p"), None);
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("older")));
        assert_eq!(c.size(), 5);
    }

    #[test]
    fn update_both_moves_a_value_between_the_slots_in_one_step() {
        let c = CacheShard::new(100);
        c.set(Slot::Latest, "p", e("aaa"));
        let (result, evicted) = c.update_both_evicting("p", |latest, earlier| {
            assert_eq!(earlier, None);
            (Some(e("bbbb")), latest, 7)
        });
        assert_eq!(result, 7);
        assert!(evicted.is_empty());
        assert_eq!(c.get(Slot::Latest, "p"), Some(e("bbbb")));
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("aaa")));
        assert_eq!(c.size(), 7);
        c.get(Slot::Earlier, "p");
        c.update_both_evicting("p", |_, _| (Some(e("cccc")), None, ()));
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("aaa")));
    }

    #[test]
    fn the_slots_of_a_path_leave_in_least_recently_used_order() {
        let c = CacheShard::new(6);
        c.set(Slot::Earlier, "p", e("aaa"));
        c.set(Slot::Latest, "p", e("bbb"));
        c.get(Slot::Earlier, "p");
        c.set(Slot::Latest, "q", e("ccc"));
        assert_eq!(c.get(Slot::Latest, "p"), None);
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("aaa")));
        assert_eq!(c.get(Slot::Latest, "q"), Some(e("ccc")));
    }

    #[test]
    fn rejects_an_entry_larger_than_the_budget() {
        let c = CacheShard::new(2);
        c.set(L, "a", e("aaaa"));
        assert_eq!(c.get(L, "a"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn oversized_entry_does_not_evict_other_keys() {
        let c = CacheShard::new(3);
        c.set(L, "kept", e("fit"));
        c.set(L, "large", e("large"));
        assert_eq!(c.get(L, "kept"), Some(e("fit")));
        assert_eq!(c.get(L, "large"), None);
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn oversized_update_removes_old_value_and_returns_its_result() {
        let c = CacheShard::new(3);
        c.set(L, "a", e("old"));
        let result = c.update_with_result(L, "a", |_| (Some(e("large")), 42));
        assert_eq!(result, 42);
        assert_eq!(c.get(L, "a"), None);
        assert_eq!(c.size(), 0);
        c.set(L, "a", e("new"));
        assert_eq!(c.get(L, "a"), Some(e("new")));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn zero_budget_retains_no_entries_even_with_zero_weight() {
        let c = Cache::new(0);
        c.set(L, "a", e(""));
        c.update(L, "b", |_| Some(e("value")));
        assert_eq!(c.get(L, "a"), None);
        assert_eq!(c.get(L, "b"), None);
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn accounts_for_usize_max_weight() {
        let c = CacheShard::new(usize::MAX);

        c.set(L, "a", WeightedEntry(usize::MAX));
        assert_eq!(c.get(L, "a"), Some(WeightedEntry(usize::MAX)));
        assert_eq!(c.size(), usize::MAX);

        c.set(L, "a", WeightedEntry(1));
        assert_eq!(c.size(), 1);
        c.delete(L, "a");
        assert_eq!(c.size(), 0);
    }

    #[test]
    fn multi_entry_overflow_recomputes_remaining_weight() {
        let c = CacheShard::new(usize::MAX);
        c.set(L, "oldest", WeightedEntry(usize::MAX - 2));
        c.set(L, "middle", WeightedEntry(1));

        c.set(L, "newest", WeightedEntry(2));

        assert_eq!(c.get(L, "oldest"), None);
        assert_eq!(c.get(L, "middle"), Some(WeightedEntry(1)));
        assert_eq!(c.get(L, "newest"), Some(WeightedEntry(2)));
        assert_eq!(c.size(), 3);
    }

    #[test]
    fn overflow_keeps_evicting_until_remaining_weight_fits() {
        let c = CacheShard::new(usize::MAX);
        c.set(L, "oldest", WeightedEntry(1));
        c.set(L, "middle", WeightedEntry(usize::MAX - 1));

        c.set(L, "newest", WeightedEntry(2));

        assert_eq!(c.get(L, "oldest"), None);
        assert_eq!(c.get(L, "middle"), None);
        assert_eq!(c.get(L, "newest"), Some(WeightedEntry(2)));
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
        c.set(L, &s0[0], e("aaa")); // 3 bytes in shard 0
        c.set(L, &s1[0], e("bbb")); // 3 bytes in shard 1
        assert_eq!(c.size(), 6);

        // Overflowing shard 0 only evicts within shard 0; shard 1 is untouched.
        c.set(L, &s0[1], e("cccc")); // 4 bytes pushes shard 0 to 7 > 6
        assert_eq!(
            c.get(L, &s0[0]),
            None,
            "least recently used entry in shard 0 should be evicted"
        );
        assert!(c.get(L, &s0[1]).is_some());
        assert!(
            c.get(L, &s1[0]).is_some(),
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
            Box::new(move |_: Slot, key: &str, value: TestEntry| {
                sink.lock().unwrap().push((key.to_owned(), value));
            }),
        );
        let keys = keys_for_shard(0, n, 4);
        c.set(L, &keys[0], e("aaa"));
        c.set(L, &keys[1], e("bbb"));
        c.delete(L, &keys[1]);
        c.set(L, &keys[1], e("bbb"));
        c.update(L, &keys[1], |_| Some(e("ccc")));
        assert!(evicted.lock().unwrap().is_empty());

        c.set(L, &keys[2], e("ddd"));
        c.set(L, &keys[3], e("too large"));
        assert_eq!(
            *evicted.lock().unwrap(),
            vec![
                (keys[0].clone(), e("aaa")),
                (keys[3].clone(), e("too large"))
            ]
        );
        let mut entries: Vec<_> = c
            .entries()
            .into_iter()
            .map(|(_, path, value)| (path, value))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = vec![(keys[1].clone(), e("ccc")), (keys[2].clone(), e("ddd"))];
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(entries, expected);
    }
}
