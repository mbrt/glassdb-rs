//! Materialized transactional key-scan requests and pages.

use glassdb_trans::ScanRange;

use crate::Error;

/// Describes one forward scan over a collection's raw key bytes.
///
/// The descriptor borrows its bounds and is [`Copy`], so the same scan can be
/// reused directly by every attempt of a retryable transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyScan<'a> {
    bounds: ScanBounds<'a>,
    after: Option<&'a [u8]>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanBounds<'a> {
    Range { start: &'a [u8], end: &'a [u8] },
    Prefix(&'a [u8]),
    All,
}

impl<'a> KeyScan<'a> {
    /// Scans the half-open raw-key range `[start, end)`.
    pub fn range(start: &'a [u8], end: &'a [u8]) -> Self {
        Self {
            bounds: ScanBounds::Range { start, end },
            after: None,
            limit: None,
        }
    }

    /// Scans every key beginning with `prefix`.
    pub fn prefix(prefix: &'a [u8]) -> Self {
        Self {
            bounds: ScanBounds::Prefix(prefix),
            after: None,
            limit: None,
        }
    }

    /// Scans every key in the collection.
    pub fn all() -> Self {
        Self {
            bounds: ScanBounds::All,
            after: None,
            limit: None,
        }
    }

    /// Excludes `key` and every smaller key from this scan.
    #[must_use]
    pub fn after(mut self, key: &'a [u8]) -> Self {
        self.after = Some(key);
        self
    }

    /// Limits the materialized page to at most `limit` keys.
    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn normalize(&self) -> Result<ScanRange, Error> {
        let (start, end) = match self.bounds {
            ScanBounds::Range { start, end } => (start, Some(end.to_vec())),
            ScanBounds::Prefix(prefix) => (prefix, prefix_end(prefix)),
            ScanBounds::All => (&[][..], None),
        };
        if end.as_deref().is_some_and(|end| start > end) {
            return Err(Error::InvalidInput(
                "scan range start must not exceed its end".into(),
            ));
        }

        let (start, start_exclusive) = match self.after {
            Some(after) if after >= start => (after.to_vec(), true),
            _ => (start.to_vec(), false),
        };
        Ok(ScanRange {
            start,
            start_exclusive,
            end,
            limit: self.limit,
        })
    }
}

/// One materialized page of sorted collection keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPage {
    keys: Vec<Vec<u8>>,
    has_next_after: bool,
}

impl KeyPage {
    /// Returns the keys in this page.
    pub fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    /// Consumes the page and returns its keys.
    pub fn into_keys(self) -> Vec<Vec<u8>> {
        self.keys
    }

    /// Returns the number of keys in this page.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Reports whether this page contains no keys.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns the exclusive lower bound to use for the next page.
    ///
    /// A returned key means this page filled its requested limit; it does not
    /// guarantee that another key exists.
    pub fn next_after(&self) -> Option<&[u8]> {
        self.has_next_after
            .then(|| self.keys.last().map(Vec::as_slice))
            .flatten()
    }

    pub(crate) fn new(keys: Vec<Vec<u8>>, limit: Option<usize>) -> Self {
        let has_next_after = limit.is_some_and(|limit| limit != 0 && keys.len() == limit);
        Self {
            keys,
            has_next_after,
        }
    }
}

impl IntoIterator for KeyPage {
    type Item = Vec<u8>;
    type IntoIter = std::vec::IntoIter<Vec<u8>>;

    fn into_iter(self) -> Self::IntoIter {
        self.keys.into_iter()
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] != u8::MAX {
            end[i] += 1;
            end.truncate(i + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_end_handles_carry_and_unbounded_prefixes() {
        assert_eq!(prefix_end(b"ab\xfe"), Some(b"ab\xff".to_vec()));
        assert_eq!(prefix_end(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(b"\xff\xff"), None);
        assert_eq!(prefix_end(b""), None);
    }

    #[test]
    fn after_normalizes_against_the_inclusive_start() {
        let before = KeyScan::range(b"b", b"z").after(b"a").normalize().unwrap();
        assert_eq!(before.start, b"b");
        assert!(!before.start_exclusive);

        let within = KeyScan::range(b"b", b"z").after(b"m").normalize().unwrap();
        assert_eq!(within.start, b"m");
        assert!(within.start_exclusive);
    }
}
