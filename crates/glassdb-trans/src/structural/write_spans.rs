//! The write spans of leaves, which keep the keys that transactions commit
//! together in one leaf (ADR-084).
//!
//! Each committed entry of a leaf names its writer, so the entries of one
//! transaction are the keys that it committed together. The decisions read
//! them from the leaves, which every database instance sees alike.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use glassdb_data::TxId;
use glassdb_storage::{LeafBody, Node, NodeSizePolicy};

/// The age of a writer after which its entries no longer count as keys that
/// transactions commit together. A transaction that wrote many keys once, for
/// example a bulk load, does not hold the leaves apart for longer.
const SPAN_AGE: Duration = Duration::from_secs(60);

/// Where a split over the soft cap divides a leaf.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SoftCapSplit {
    /// At the median key, which divides no write span.
    Median,
    /// At this key, which divides fewer write spans than the median.
    At(Vec<u8>),
    /// Not now: each key of the middle half divides a write span.
    Wait,
}

/// The first and the last key of the entries of one recent writer of two or
/// more of them, and the leaves of these entries.
#[derive(Debug, Clone, Copy)]
struct Span<'a> {
    first: &'a [u8],
    last: &'a [u8],
    in_left: bool,
    in_right: bool,
}

/// Returns where the soft cap split of the leaf `node` divides it, at `now`.
///
/// The split moves from the median to the nearest key of the middle half of
/// the leaf that divides no write span, or waits. A leaf over twice the soft
/// cap of entries, or over the byte soft cap, does not wait: it splits at the
/// key of the middle half that divides the fewest write spans.
pub(super) fn soft_cap_split(
    node: &Node,
    policy: &NodeSizePolicy,
    now: SystemTime,
) -> SoftCapSplit {
    let Some(leaf) = node.as_leaf() else {
        return SoftCapSplit::Median;
    };
    let Some(median) = leaf.median_key() else {
        return SoftCapSplit::Median;
    };
    let spans = spans(leaf, None, now);
    if divided(&spans, median) == 0 {
        return SoftCapSplit::Median;
    }
    let keys: Vec<&[u8]> = leaf.entries().map(|entry| entry.key.as_slice()).collect();
    let median_index = keys.len() / 2;
    let candidates = middle_half(&keys)
        .filter(|(_, key)| leaf.divides_at(key))
        .map(|(index, key)| (divided(&spans, key), index.abs_diff(median_index), key));
    let forced = leaf.len() > policy.leaf_max_entries().saturating_mul(2)
        || node.content_encoded_len() > policy.node_soft_max_bytes();
    let best = if forced {
        candidates.min_by_key(|(divided, distance, _)| (*divided, *distance))
    } else {
        candidates
            .filter(|(divided, _, _)| *divided == 0)
            .min_by_key(|(_, distance, _)| *distance)
    };
    match best {
        Some((_, _, key)) if key == median => SoftCapSplit::Median,
        Some((_, _, key)) => SoftCapSplit::At(key.to_vec()),
        None if forced => SoftCapSplit::Median,
        None => SoftCapSplit::Wait,
    }
}

/// Reports whether write spans pay for the merge of the leaf `left` into its
/// right sibling `right`, at `now`.
///
/// The write spans over the boundary must be at least half of the write spans
/// of the two leaves, because a merge for a few of them divides the others
/// again at the next split. And when the merged leaf is over the soft cap, a
/// key of its middle half must divide no write span, so that its next split
/// keeps them.
pub(super) fn pays_for_merge(
    left: &Node,
    right: &Node,
    policy: &NodeSizePolicy,
    now: SystemTime,
) -> bool {
    let (Some(left), Some(right)) = (left.as_leaf(), right.as_leaf()) else {
        return false;
    };
    let spans = spans(left, Some(right), now);
    let crossing = spans
        .iter()
        .filter(|span| span.in_left && span.in_right)
        .count();
    if crossing == 0 || crossing * 2 < spans.len() {
        return false;
    }
    let keys: Vec<&[u8]> = left
        .entries()
        .chain(right.entries())
        .map(|entry| entry.key.as_slice())
        .collect();
    keys.len() <= policy.leaf_max_entries()
        || middle_half(&keys).any(|(_, key)| divided(&spans, key) == 0)
}

/// Returns the write spans of the entries of `left`, and of `right` after it,
/// whose writers started less than [`SPAN_AGE`] before `now`.
///
/// The clock of another database instance can be ahead of this one, so a
/// writer that started less than [`SPAN_AGE`] after `now` is recent too.
fn spans<'a>(left: &'a LeafBody, right: Option<&'a LeafBody>, now: SystemTime) -> Vec<Span<'a>> {
    let recent = |writer: &TxId| match now.duration_since(writer.started_at()) {
        Ok(age) => age < SPAN_AGE,
        Err(ahead) => ahead.duration() < SPAN_AGE,
    };
    let mut writers: HashMap<&TxId, (Span<'a>, usize)> = HashMap::new();
    let entries = left.entries().map(|entry| (entry, true)).chain(
        right
            .into_iter()
            .flat_map(LeafBody::entries)
            .map(|entry| (entry, false)),
    );
    for (entry, in_left) in entries {
        let Some(writer) = entry.current.writer().filter(|writer| recent(writer)) else {
            continue;
        };
        let key = entry.key.as_slice();
        let (span, count) = writers.entry(writer).or_insert((
            Span {
                first: key,
                last: key,
                in_left: false,
                in_right: false,
            },
            0,
        ));
        span.last = key;
        span.in_left |= in_left;
        span.in_right |= !in_left;
        *count += 1;
    }
    writers
        .into_values()
        .filter(|(_, count)| *count >= 2)
        .map(|(span, _)| span)
        .collect()
}

/// Returns the number of the `spans` that a split at `key` divides.
fn divided(spans: &[Span<'_>], key: &[u8]) -> usize {
    spans
        .iter()
        .filter(|span| span.first < key && key <= span.last)
        .count()
}

/// Returns the keys of the middle half of the sorted `keys`, with their index,
/// so that both halves of a split there keep a quarter of the keys.
fn middle_half<'a>(keys: &'a [&'a [u8]]) -> impl Iterator<Item = (usize, &'a [u8])> {
    let quarter = keys.len() / 4;
    keys.iter()
        .copied()
        .enumerate()
        .skip(quarter.max(1))
        .take(keys.len().saturating_sub(2 * quarter.max(1)))
}

#[cfg(test)]
mod tests;
