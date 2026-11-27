//! A thread-safe, byte-weighted cache that evicts entries in an order near
//! least-recently-used. Ported from the Go `internal/cache` package.
//!
//! The cache is partitioned into independent shards, each with its own lock and
//! byte budget, to reduce lock contention on this hot DB-level structure.
//!
//! A hit changes no order: it takes the shared lock of its shard and sets the
//! access bit of its entry. So the readers of a hot path, such as the root of a
//! tree, do not wait for each other. The entries are in insertion order, and
//! eviction gives a second chance to each entry with its access bit set: it
//! clears the bit and moves the entry to the back (the CLOCK algorithm).
//!
//! Each path has two slots: its latest state and its earlier states (ADR-103).
//! The two slots of a path are separate entries in one eviction order, and they
//! are always in the same shard.

use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use foldhash::fast::FixedState;
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

/// Hashes the paths to their shards. The seed is fixed, so that the shard of
/// a path, and so the entries that a shard evicts, do not change between
/// runs. It differs from the seed of the entry maps, which hash `(slot, path)`.
const SHARD_HASHER: FixedState = FixedState::with_seed(0x5eed_5ba2_d000_0001);

/// Hashes the keys of the entry map of each shard. The eviction order does not
/// depend on it.
const MAP_HASHER: FixedState = FixedState::with_seed(0x5eed_5ba2_d000_0002);

/// Returns the hash that selects the shard of `path`.
fn shard_hash<Q: Hash + ?Sized>(path: &Q) -> u64 {
    SHARD_HASHER.hash_one(path)
}

/// The key of one entry. It hashes as `(slot, path)`, so a lookup can hash a
/// borrowed path and compare it without an allocation.
#[derive(PartialEq, Eq)]
struct Key<K> {
    slot: Slot,
    path: K,
}

impl<K: Hash> Hash for Key<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self.slot, &self.path).hash(state);
    }
}

impl<K> Key<K> {
    fn is<Q>(&self, slot: Slot, path: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + ?Sized,
    {
        self.slot == slot && self.path.borrow() == path
    }
}

/// An entry that the cache evicted for capacity.
type Evicted<K, V> = (Slot, K, V);

/// A value and whether it was used after the eviction last passed it.
struct Entry<V> {
    value: V,
    accessed: AtomicBool,
}

impl<V> Entry<V> {
    /// A new value counts as used, as in least-recently-used order, so that
    /// the eviction that makes room for it does not remove it first.
    fn new(value: V) -> Self {
        Self {
            value,
            accessed: AtomicBool::new(true),
        }
    }

    /// Sets the access bit. It writes only a clear bit, so that the hits of a
    /// hot entry do not write to its memory.
    fn mark(&self) {
        if !self.accessed.load(Ordering::Relaxed) {
            self.accessed.store(true, Ordering::Relaxed);
        }
    }
}

struct Inner<K, V> {
    max_size: usize,
    curr_size: usize,
    /// Entries in eviction order: the front is the next to evict unless its
    /// access bit is set, and the back is the newest. The linked list gives
    /// O(1) second chances and evictions, and eviction follows that list order
    /// (not the hash buckets), so behavior is independent of the hasher.
    map: LinkedHashMap<Key<K>, Entry<V>, FixedState>,
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

impl<K: Hash + Eq, V: Weighable + Clone> Inner<K, V> {
    fn hash<Q>(&self, slot: Slot, path: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.map.hasher().hash_one((slot, path))
    }

    fn peek<Q>(&self, slot: Slot, path: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(slot, path);
        self.map
            .raw_entry()
            .from_hash(hash, |key| key.is(slot, path))
            .map(|(_, entry)| &entry.value)
    }

    /// Returns the value of a slot, and sets its access bit.
    fn hit<Q>(&self, slot: Slot, path: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(slot, path);
        let (_, entry) = self
            .map
            .raw_entry()
            .from_hash(hash, |key| key.is(slot, path))?;
        entry.mark();
        Some(&entry.value)
    }

    fn delete_entry<Q>(&mut self, slot: Slot, path: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash(slot, path);
        if let RawEntryMut::Occupied(entry) = self
            .map
            .raw_entry_mut()
            .from_hash(hash, |key| key.is(slot, path))
        {
            let old = entry.remove();
            self.curr_size = replace_weight(self.curr_size, old.value.size(), 0).size;
        }
    }

    /// Stores `new` in a slot, or removes the slot when `new` is `None` or
    /// does not fit the budget. Returns whether the total weight overflowed.
    /// The caller evicts for capacity after it stores every slot.
    fn put<Q>(
        &mut self,
        slot: Slot,
        path: &Q,
        old_size: usize,
        new: Option<V>,
        evicted: &mut Vec<Evicted<K, V>>,
    ) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        let Some(new) = new else {
            // Remove an existing entry, or leave an absent one absent.
            self.delete_entry(slot, path);
            return false;
        };
        let new_size = new.size();
        if self.max_size == 0 || new_size > self.max_size {
            // Keeping an old value here could hide a completed mutation.
            self.delete_entry(slot, path);
            evicted.push((slot, path.to_owned(), new));
            return false;
        }
        let replacement = replace_weight(self.curr_size, old_size, new_size);
        self.curr_size = replacement.size;
        // `insert` appends at the back (the newest) and, for an existing key,
        // moves it there while replacing the value.
        self.map.insert(
            Key {
                slot,
                path: path.to_owned(),
            },
            Entry::new(new),
        );
        replacement.overflowed
    }

    fn recompute_weight(&mut self) -> bool {
        let mut curr_size = 0;
        let mut overflowed = false;
        for entry in self.map.values() {
            let replacement = replace_weight(curr_size, 0, entry.value.size());
            curr_size = replacement.size;
            overflowed = replacement.overflowed;
            if overflowed {
                break;
            }
        }
        self.curr_size = curr_size;
        overflowed
    }

    fn remove_oldest(&mut self, mut overflowed: bool, evicted: &mut Vec<Evicted<K, V>>) {
        while overflowed || self.curr_size > self.max_size {
            let Some((key, mut entry)) = self.map.pop_front() else {
                return;
            };
            if *entry.accessed.get_mut() {
                // A second chance. The bit is clear when the eviction comes
                // back to the entry, so the loop ends.
                *entry.accessed.get_mut() = false;
                self.map.insert(key, entry);
                continue;
            }
            let v = entry.value;
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
/// eviction order, and byte budget. A hit takes the lock shared, and a change
/// takes it exclusive.
pub struct CacheShard<K, V> {
    inner: RwLock<Inner<K, V>>,
}

impl<K: Hash + Eq + Clone, V: Weighable + Clone> CacheShard<K, V> {
    /// Creates a cache shard with the given maximum size in bytes.
    pub fn new(max_size: usize) -> Self {
        CacheShard {
            inner: RwLock::new(Inner {
                max_size,
                curr_size: 0,
                map: LinkedHashMap::with_hasher(MAP_HASHER),
            }),
        }
    }

    fn get<Q>(&self, slot: Slot, path: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.read().unwrap().hit(slot, path).cloned()
    }

    /// Updates one slot and returns the entries that the update evicted for
    /// capacity, including a new value that does not fit.
    fn update_evicting<Q, F, R>(&self, slot: Slot, path: &Q, f: F) -> (R, Vec<Evicted<K, V>>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        F: FnOnce(Option<V>) -> (Option<V>, R),
    {
        let mut inner = self.inner.write().unwrap();
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
    fn update_both_evicting<Q, F, R>(&self, path: &Q, f: F) -> (R, Vec<Evicted<K, V>>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        F: FnOnce(Option<V>, Option<V>) -> (Option<V>, Option<V>, R),
    {
        let mut inner = self.inner.write().unwrap();
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

    fn delete<Q>(&self, slot: Slot, path: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.write().unwrap().delete_entry(slot, path);
    }

    fn size(&self) -> usize {
        self.inner.read().unwrap().curr_size
    }

    fn entries(&self) -> Vec<(Slot, K, V)> {
        let inner = self.inner.read().unwrap();
        inner
            .map
            .iter()
            .map(|(key, entry)| (key.slot, key.path.clone(), entry.value.clone()))
            .collect()
    }
}

/// Receives each entry that the cache evicts for capacity. The cache calls it
/// after it releases the shard lock.
pub type EvictionHook<K, V> = Box<dyn Fn(Slot, &K, V) + Send + Sync>;

/// A thread-safe cache that evicts entries in an order near least-recently-used
/// (see the module) once the total size exceeds the configured maximum. It is partitioned into
/// independent shards, each with its own lock and byte budget. The entries are
/// keyed by paths of type `K`, and a lookup can use any borrowed form of `K`.
pub struct Cache<K, V> {
    sh: Sharded<CacheShard<K, V>>,
    on_evict: Option<EvictionHook<K, V>>,
}

impl<K: Hash + Eq + Clone, V: Weighable + Clone> Cache<K, V> {
    /// Creates a cache with the given maximum size in bytes. The budget is split
    /// evenly across shards to reduce lock contention. Zero disables retention.
    pub fn new(max_size: usize) -> Self {
        Self::build(max_size, None)
    }

    /// Creates a cache like [`Self::new`] that passes each entry it evicts for
    /// capacity to `on_evict`. A new value that does not fit its shard's
    /// budget counts as evicted. Explicit removals and replacements do not.
    pub fn with_eviction(max_size: usize, on_evict: EvictionHook<K, V>) -> Self {
        Self::build(max_size, Some(on_evict))
    }

    fn build(max_size: usize, on_evict: Option<EvictionHook<K, V>>) -> Self {
        let per = max_size / shard::count();
        Cache {
            sh: Sharded::new(move |_| CacheShard::new(per)),
            on_evict,
        }
    }

    /// The shard of both slots of `path`.
    fn shard<Q>(&self, path: &Q) -> &CacheShard<K, V>
    where
        K: Borrow<Q>,
        Q: Hash + ?Sized,
    {
        self.sh.for_hash(shard_hash(path))
    }

    /// Returns the value in a slot of `path`, and marks it as used.
    pub fn get<Q>(&self, slot: Slot, path: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shard(path).get(slot, path)
    }

    /// Updates a slot of `path` while holding the lock. The closure receives
    /// the old value (or `None`) and returns the new value, or `None` to remove
    /// the entry. A new value that exceeds its shard's budget also removes the
    /// entry.
    pub fn update<Q, F>(&self, slot: Slot, path: &Q, f: F)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        F: FnOnce(Option<V>) -> Option<V>,
    {
        self.update_with_result(slot, path, |old| (f(old), ()));
    }

    /// Removes a slot of `path`.
    pub fn delete<Q>(&self, slot: Slot, path: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
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
    pub fn entries(&self) -> Vec<(Slot, K, V)> {
        let mut entries = Vec::new();
        self.sh.each(|s| entries.extend(s.entries()));
        entries
    }

    /// Atomically updates a slot of `path` and returns an auxiliary result
    /// produced by the update closure.
    pub(crate) fn update_with_result<Q, F, R>(&self, slot: Slot, path: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
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
    pub(crate) fn update_both<Q, F, R>(&self, path: &Q, f: F) -> R
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
        F: FnOnce(Option<V>, Option<V>) -> (Option<V>, Option<V>, R),
    {
        let (result, evicted) = self.shard(path).update_both_evicting(path, f);
        self.evicted(evicted);
        result
    }

    fn evicted(&self, evicted: Vec<Evicted<K, V>>) {
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

    impl<V: Weighable + Clone> TestUpdates<V> for CacheShard<String, V> {
        fn update_with_result<R>(
            &self,
            slot: Slot,
            path: &str,
            f: impl FnOnce(Option<V>) -> (Option<V>, R),
        ) -> R {
            self.update_evicting(slot, path, f).0
        }
    }

    impl<V: Weighable + Clone> TestUpdates<V> for Cache<String, V> {
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
        let c: CacheShard<String, TestEntry> = CacheShard::new(100);
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
    fn a_hit_gives_the_oldest_entry_a_second_chance() {
        // Budget for two 3-byte entries.
        let c = CacheShard::new(6);
        c.set(L, "a", e("aaa"));
        c.set(L, "b", e("bbb"));
        // The eviction for "c" clears the bits of "b" and "c", and removes
        // "a", the oldest.
        c.set(L, "c", e("ccc"));
        assert_eq!(c.size(), 6);
        c.get(L, "b");
        // "b" is now the oldest, but a hit read it, so "c" goes first.
        c.set(L, "d", e("ddd"));
        assert_eq!(c.get(L, "c"), None);
        assert_eq!(c.get(L, "b"), Some(e("bbb")));
        assert_eq!(c.get(L, "d"), Some(e("ddd")));
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
    fn the_slots_of_a_path_leave_separately() {
        let c = CacheShard::new(6);
        c.set(Slot::Earlier, "p", e("aaa"));
        c.set(Slot::Latest, "p", e("bbb"));
        // The earlier slot is the oldest entry, so it leaves alone.
        c.set(Slot::Latest, "q", e("ccc"));
        assert_eq!(c.get(Slot::Earlier, "p"), None);
        assert_eq!(c.get(Slot::Latest, "p"), Some(e("bbb")));
        // The latest slot of "p" is now the oldest, and a hit read it, so
        // "q" leaves first.
        c.set(Slot::Earlier, "p", e("ddd"));
        assert_eq!(c.get(Slot::Latest, "q"), None);
        assert_eq!(c.get(Slot::Latest, "p"), Some(e("bbb")));
        assert_eq!(c.get(Slot::Earlier, "p"), Some(e("ddd")));
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
            if (shard_hash(k.as_str()) as usize) & (n - 1) == target {
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
        let evicted = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = evicted.clone();
        let c = Cache::with_eviction(
            6 * n,
            Box::new(move |_: Slot, key: &String, value: TestEntry| {
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
