//! Budgets bounding how much committed value data leaf entries carry inline
//! (ADR-051).

use crate::shard::ShardEntry;

/// Default largest value that may be inlined. Small enough that the extra bytes
/// on every leaf CAS stay cheap next to the transaction-object read they save.
const DEFAULT_MAX_VALUE_BYTES: usize = 1024;

/// Default largest aggregate inline payload one leaf may carry.
const DEFAULT_MAX_LEAF_BYTES: usize = 64 * 1024;

/// How much committed value data a leaf may carry inline.
///
/// Both budgets are *admission-only*: a value that misses either is published
/// as an external pointer, so inlining never blocks a lock release or delays
/// convergence. The budgets are a runtime tuning knob, never persisted, and
/// values already inline are grandfathered — lowering a budget leaves them
/// alone, because an inline value may be a key's only copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InlinePolicy {
    /// Largest value that may be inlined at all.
    pub max_value_bytes: usize,
    /// Largest aggregate inline payload one leaf may carry.
    pub max_leaf_bytes: usize,
}

impl Default for InlinePolicy {
    fn default() -> Self {
        InlinePolicy {
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_leaf_bytes: DEFAULT_MAX_LEAF_BYTES,
        }
    }
}

impl InlinePolicy {
    /// The policy that admits nothing, so every value is published as an
    /// external pointer.
    pub fn none() -> Self {
        InlinePolicy {
            max_value_bytes: 0,
            max_leaf_bytes: 0,
        }
    }

    /// Reports whether `value_len` bytes may be inlined for `key` in a leaf
    /// holding `leaf`.
    ///
    /// The key's own inline payload is replaced rather than added to, so
    /// overwriting an inline value with one of the same size always readmits.
    pub fn admits<'a>(
        &self,
        leaf: impl IntoIterator<Item = &'a ShardEntry>,
        key: &[u8],
        value_len: usize,
    ) -> bool {
        // A zero per-value budget disables inlining outright, so an empty value
        // is rejected too rather than slipping through as zero bytes.
        if self.max_value_bytes == 0 || value_len > self.max_value_bytes {
            return false;
        }
        let others: usize = leaf
            .into_iter()
            .filter(|entry| entry.key != key)
            .map(|entry| entry.current.inline_len())
            .sum();
        others + value_len <= self.max_leaf_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_data::TxId;

    use super::*;
    use crate::shard::CurrentState;

    fn inline_entry(key: &[u8], len: usize) -> ShardEntry {
        ShardEntry {
            current: CurrentState::Inline {
                writer: TxId::with_priority(1, key),
                value: Arc::from(vec![0u8; len]),
            },
            ..ShardEntry::new(key)
        }
    }

    #[test]
    fn a_value_over_the_per_value_budget_is_never_admitted() {
        let policy = InlinePolicy {
            max_value_bytes: 8,
            max_leaf_bytes: 1024,
        };
        assert!(policy.admits([].iter(), b"k", 8));
        assert!(!policy.admits([].iter(), b"k", 9));
    }

    #[test]
    fn the_leaf_budget_counts_every_other_entry() {
        let policy = InlinePolicy {
            max_value_bytes: 100,
            max_leaf_bytes: 30,
        };
        let leaf = [inline_entry(b"a", 10), inline_entry(b"b", 10)];

        assert!(policy.admits(leaf.iter(), b"c", 10));
        assert!(!policy.admits(leaf.iter(), b"c", 11));
    }

    // The budget a resolver is re-asked with when its inline stage does not
    // fit: nothing is admitted, not even an empty value.
    #[test]
    fn the_empty_policy_admits_nothing() {
        let policy = InlinePolicy::none();

        assert!(!policy.admits([].iter(), b"k", 0));
        assert!(!policy.admits([].iter(), b"k", 1));
    }

    // Overwriting a key's own inline value replaces it, so a leaf sitting
    // exactly at its budget still readmits the same-sized value.
    #[test]
    fn a_keys_own_inline_payload_is_replaced_not_added() {
        let policy = InlinePolicy {
            max_value_bytes: 100,
            max_leaf_bytes: 20,
        };
        let leaf = [inline_entry(b"a", 10), inline_entry(b"b", 10)];

        assert!(policy.admits(leaf.iter(), b"b", 10));
        assert!(!policy.admits(leaf.iter(), b"b", 11));
    }
}
