//! The contended keys of a database instance, and the leaves that keep them
//! apart from the keys of other database instances (ADR-085).
//!
//! A database instance that loses leaf CASes on a key to other database
//! instances splits the key away from their keys, and merges only the leaves
//! that it writes. The decisions read the writers of the leaf entries, and each
//! database instance judges them with the identities of its own commits.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use glassdb_concurr::rt;
use glassdb_data::CollectionAddress;
use glassdb_storage::Node;
use hashlink::LinkedHashMap;

use super::write_spans::{self, SPAN_AGE};
use crate::monitor::{LOCAL_RETENTION, Monitor};

// A writer of a leaf entry that is recent is local for as long as it is
// recent.
const _: () = assert!(LOCAL_RETENTION.as_secs() >= SPAN_AGE.as_secs());

/// The half-life of the share of a key in the lost leaf CASes.
const SHARE_HALF_LIFE: Duration = Duration::from_secs(10);

/// The decayed share above which a key is contended. A database instance that
/// loses about two leaf CASes each second on the key alone reaches it. The
/// keys of the random loads that were measured stayed below two thirds of it.
const CONTENDED_SHARE: f64 = 32.0;

/// The keys whose shares one database instance keeps. The key with the oldest
/// lost leaf CAS goes first.
const MAX_SHARED_KEYS: usize = 4096;

/// The decayed shares of the keys of one database instance in its lost leaf
/// CASes. Its monitor tells its local writers.
pub(super) struct Contention {
    shares: Mutex<LinkedHashMap<(CollectionAddress, Vec<u8>), Share>>,
    mon: Monitor,
}

/// The decayed share of one key, at the instant of its last lost leaf CAS.
struct Share {
    value: f64,
    at: rt::Instant,
}

/// Who wrote the live value of one leaf entry, for one database instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Writer {
    Local,
    Foreign,
    /// The writer is not recent, or the entry has no live value. A contended
    /// key has a live value, because database instances keep writing it. A
    /// tombstone holds no key apart, so the leaves of deleted keys merge.
    NotRecent,
}

impl Contention {
    /// Returns the contention of the database instance of `mon`, with no
    /// lost leaf CAS yet.
    pub(super) fn new(mon: Monitor) -> Self {
        Self {
            shares: Mutex::default(),
            mon,
        }
    }

    /// Notes a lost leaf CAS of a round on the keys `round_keys` of
    /// `collection`, and returns the key of the round that is contended now,
    /// if any. Each key of the round gets an equal share of the loss.
    pub(super) fn lost_leaf_cas(
        &self,
        collection: &CollectionAddress,
        round_keys: &BTreeSet<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        let share = 1.0 / round_keys.len().max(1) as f64;
        let now = rt::Instant::now();
        let mut shares = self.shares.lock().unwrap();
        let mut contended: Option<(&Vec<u8>, f64)> = None;
        for key in round_keys {
            let id = (collection.clone(), key.clone());
            let value = shares.remove(&id).map_or(0.0, |old| {
                old.value * decay(now.saturating_duration_since(old.at))
            }) + share;
            shares.insert(id, Share { value, at: now });
            if value >= CONTENDED_SHARE && contended.is_none_or(|(_, top)| value > top) {
                contended = Some((key, value));
            }
        }
        while shares.len() > MAX_SHARED_KEYS {
            shares.pop_front();
        }
        contended.map(|(key, _)| key.clone())
    }

    /// Returns the key at which a split of the leaf `node` separates the
    /// contended key `key` from the keys of other database instances, at
    /// `now`.
    ///
    /// The split key is at an edge of the run of keys around `key` that have
    /// no live value of a recent foreign writer, and divides no write span. The edge
    /// with more keys outside the run comes first, so that each split moves
    /// most of the other keys away. `key` is in the run whoever wrote it last:
    /// a database instance that loses every leaf CAS on it has not written
    /// it yet. There is none when the run is the whole leaf.
    pub(super) fn split_key(&self, node: &Node, key: &[u8], now: SystemTime) -> Option<Vec<u8>> {
        let leaf = node.as_leaf()?;
        let writers = self.writers(node, now);
        let index = writers.iter().position(|(entry, _)| *entry == key)?;
        let in_run = |writer: &Writer| *writer != Writer::Foreign;
        let start = writers[..index]
            .iter()
            .rposition(|(_, writer)| !in_run(writer))
            .map_or(0, |foreign| foreign + 1);
        let end = writers[index + 1..]
            .iter()
            .position(|(_, writer)| !in_run(writer))
            .map_or(writers.len(), |foreign| index + 1 + foreign);
        let mut edges = Vec::with_capacity(2);
        if start > 0 {
            edges.push((start, writers[start].0));
        }
        if end < writers.len() {
            edges.push((writers.len() - end, writers[end].0));
        }
        edges.sort_by_key(|(outside, _)| std::cmp::Reverse(*outside));
        edges
            .into_iter()
            .map(|(_, edge)| edge)
            .find(|edge| leaf.divides_at(edge) && write_spans::divides_no_span(node, edge, now))
            .map(<[u8]>::to_vec)
    }

    /// Reports whether an underfull merge of the leaf `left` into its right
    /// sibling `right` must wait, at `now`.
    ///
    /// It waits when both leaves have live values of recent writers, and the
    /// writers of one of them are all foreign. This database instance then
    /// writes none of the keys of that leaf, so it cannot tell whether the
    /// merge puts the keys of two database instances back together. The
    /// writers of the leaf decide. A leaf without live values of recent
    /// writers holds no key apart.
    pub(super) fn keeps_apart(&self, left: &Node, right: &Node, now: SystemTime) -> bool {
        let writers = |node: &Node| -> (bool, bool) {
            let writers = self.writers(node, now);
            let has = |kind| writers.iter().any(|(_, writer)| *writer == kind);
            (has(Writer::Local), has(Writer::Foreign))
        };
        match (writers(left), writers(right)) {
            ((false, false), _) | (_, (false, false)) => false,
            ((left_local, _), (right_local, _)) => !left_local || !right_local,
        }
    }

    /// Returns the writer of the live value of each entry of the leaf `node`,
    /// at `now`.
    fn writers<'a>(&self, node: &'a Node, now: SystemTime) -> Vec<(&'a [u8], Writer)> {
        let Some(leaf) = node.as_leaf() else {
            return Vec::new();
        };
        leaf.entries()
            .map(|entry| {
                let writer = match entry.current.writer() {
                    Some(writer) if entry.exists() && write_spans::is_recent(writer, now) => {
                        if self.mon.is_recent_local(writer) {
                            Writer::Local
                        } else {
                            Writer::Foreign
                        }
                    }
                    _ => Writer::NotRecent,
                };
                (entry.key.as_slice(), writer)
            })
            .collect()
    }
}

/// Returns the factor by which a share decays in `elapsed`.
fn decay(elapsed: Duration) -> f64 {
    0.5_f64.powf(elapsed.as_secs_f64() / SHARE_HALF_LIFE.as_secs_f64())
}
