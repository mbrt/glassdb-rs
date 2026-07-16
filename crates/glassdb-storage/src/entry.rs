//! The cache-size seed shared by the storage caches. The coordination-object
//! cache lives in the decoded [`crate::CachedStore`] (ADR-036), which is sized
//! from this budget; user values are derived from cached transaction objects
//! rather than a separate cache.

/// A byte budget for a cache, passed to the stores that allocate from it.
#[derive(Clone, Copy)]
pub struct SharedCache {
    max_size_b: usize,
}

impl SharedCache {
    /// Creates a cache budget of at most `max_size_b` bytes.
    pub fn new(max_size_b: usize) -> Self {
        SharedCache { max_size_b }
    }

    /// The byte budget a store should size itself to.
    pub(crate) fn max_size_b(&self) -> usize {
        self.max_size_b
    }
}
