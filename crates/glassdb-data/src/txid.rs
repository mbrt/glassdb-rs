use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use glassdb_concurr::entropy::fill_bytes;

use crate::ID_BYTES;

/// Offset of the big-endian UnixNano timestamp within the ID.
const TX_ID_TS_OFF: usize = 8;

/// A transaction identity.
///
/// `TxId` is the short Rust name for `TransactionIdentity`.
///
/// The layout is `[8 bytes random][8 bytes big-endian UnixNano timestamp]`. The
/// random bytes come first so that transaction-record paths keep a high-entropy
/// prefix, spreading writes across object-storage partitions instead of
/// clustering sequential commits into a single hot partition. The timestamp
/// suffix encodes the transaction priority used by the wound-wait rule: an
/// earlier timestamp means an older, higher-priority transaction.
///
/// [`Ord`] compares the complete identity bytes for identity-keyed collections and is
/// not wound-wait priority order; use [`TxId::older`] for priority decisions.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxId([u8; ID_BYTES]);

impl TxId {
    /// Generates a new random transaction identity.
    ///
    /// The timestamp suffix is random rather than clock-derived: this keeps the
    /// `data` crate free of any clock dependency. Production code mints IDs via
    /// [`TxId::new_at`] with a clock-sourced timestamp; this constructor is for
    /// callers (mostly tests) that only need a unique identifier.
    pub fn new_random() -> Self {
        let mut bytes = [0; ID_BYTES];
        fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// Builds a transaction identity from a random prefix and an explicit instant,
    /// whose UnixNano timestamp determines the wound-wait priority. The caller
    /// supplies the instant (typically the monitor's clock), so the `data` crate
    /// never sources time itself. Instants at or before the Unix epoch saturate
    /// to priority zero (the highest priority).
    pub fn new_at(t: SystemTime) -> Self {
        let unix_nanos = t
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Self::with_random_prefix(unix_nanos)
    }

    /// Builds a transaction identity from an explicit timestamp and random prefix.
    /// Meant for tests that need deterministic priorities. At most the first 8
    /// bytes of `prefix` are used, and shorter prefixes are padded with zeros.
    pub fn with_priority(unix_nanos: u64, prefix: &[u8]) -> Self {
        let mut bytes = [0; ID_BYTES];
        let n = prefix.len().min(TX_ID_TS_OFF);
        bytes[..n].copy_from_slice(&prefix[..n]);
        bytes[TX_ID_TS_OFF..].copy_from_slice(&unix_nanos.to_be_bytes());
        Self(bytes)
    }

    /// Returns a transaction identity that preserves the priority of
    /// `self` but uses a fresh random prefix. A wounded transaction reuses its
    /// priority on identity renewal to avoid starvation, while the new prefix
    /// gives it a distinct transaction record that lands in a different storage
    /// partition.
    pub fn renew(&self) -> Self {
        Self::with_random_prefix(self.priority())
    }

    /// Reports whether `self` has strictly higher priority than `other`, i.e. it
    /// carries an earlier timestamp.
    ///
    /// Priority depends only on the timestamp, never on the random prefix. This
    /// is essential for the wound-wait rule: [`TxId::renew`] preserves the
    /// timestamp but mints a fresh prefix on every restart, so a prefix-based
    /// tiebreak would let two equal-timestamp transactions flip their relative
    /// order on each wound and livelock by wounding each other forever.
    /// Transactions sharing a timestamp are therefore never ordered against each
    /// other; that rare tie is left to the serial-acquisition deadlock safety net.
    pub fn older(&self, other: &TxId) -> bool {
        self.priority() < other.priority()
    }

    /// Builds a transaction identity from its exact 16-byte representation.
    pub const fn from_bytes(bytes: [u8; ID_BYTES]) -> Self {
        Self(bytes)
    }

    /// Parses an exact 16-byte transaction identity.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok().map(Self)
    }

    /// Returns the identity's 16-byte representation.
    pub const fn as_bytes(&self) -> &[u8; ID_BYTES] {
        &self.0
    }

    fn with_random_prefix(unix_nanos: u64) -> Self {
        let mut bytes = [0; ID_BYTES];
        fill_bytes(&mut bytes[..TX_ID_TS_OFF]);
        bytes[TX_ID_TS_OFF..].copy_from_slice(&unix_nanos.to_be_bytes());
        Self(bytes)
    }

    /// Returns the wound-wait priority (the big-endian UnixNano timestamp
    /// suffix).
    fn priority(&self) -> u64 {
        let mut ts = [0; ID_BYTES - TX_ID_TS_OFF];
        ts.copy_from_slice(&self.0[TX_ID_TS_OFF..]);
        u64::from_be_bytes(ts)
    }
}

impl fmt::Display for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0.iter() {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for TxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxId({self})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These run on a runtime so prefix minting works under any build; outside
    // the simulation executor the shared entropy facade uses the process RNG.
    #[tokio::test]
    async fn generated_ids_round_trip() {
        let id = TxId::new_random();
        assert_eq!(TxId::from_slice(id.as_bytes()), Some(id));
    }

    #[test]
    fn parsing_requires_the_exact_width() {
        let bytes = [7; ID_BYTES];
        assert_eq!(TxId::from_slice(&bytes), Some(TxId::from_bytes(bytes)));
        for invalid in [&[][..], &[7; ID_BYTES - 1], &[7; ID_BYTES + 1]] {
            assert_eq!(TxId::from_slice(invalid), None);
        }
    }

    #[test]
    fn display_is_lowercase_hex() {
        let id = TxId::with_priority(0x0123_4567_89ab_cdef, &[0xfe, 0xdc]);
        assert_eq!(id.to_string(), "fedc0000000000000123456789abcdef");
    }

    #[tokio::test]
    async fn new_at_layout() {
        let nanos = 1_700_000_000_000_000_000u64;
        let id = TxId::new_at(UNIX_EPOCH + std::time::Duration::from_nanos(nanos));
        assert_eq!(id.priority(), nanos);
    }

    #[test]
    fn older_timestamp_wins_over_prefix() {
        let base = 1000u64 * 1_000_000_000;
        let older = TxId::with_priority(base, b"zzzzzzzz");
        let younger = TxId::with_priority(base + 1_000_000_000, b"aaaaaaaa");
        assert!(older.older(&younger));
        assert!(!younger.older(&older));
    }

    #[test]
    fn older_timestamp_dominates_prefix() {
        let older = TxId::with_priority(1_000_000_000, &[0xff; 8]);
        let younger = TxId::with_priority(2_000_000_000, &[0x00; 8]);
        assert!(older.older(&younger));
        assert!(younger < older, "byte order is intentionally not priority");
    }

    #[test]
    fn older_equal_timestamp_not_ordered() {
        let ts = 42u64 * 1_000_000_000;
        let a = TxId::with_priority(ts, &[0, 0, 0, 0, 0, 0, 0, 1]);
        let b = TxId::with_priority(ts, &[0, 0, 0, 0, 0, 0, 0, 2]);
        // Same timestamp: neither is older, regardless of the random prefix.
        assert!(!a.older(&b));
        assert!(!b.older(&a));
    }

    #[tokio::test]
    async fn renew_does_not_flip_ordering() {
        let ts = 42u64 * 1_000_000_000;
        let mut a = TxId::with_priority(ts, &[0, 0, 0, 0, 0, 0, 0, 1]);
        let mut b = TxId::with_priority(ts, &[0, 0, 0, 0, 0, 0, 0, 2]);
        for _ in 0..100 {
            a = a.renew();
            b = b.renew();
            assert!(!a.older(&b));
            assert!(!b.older(&a));
        }
    }

    #[tokio::test]
    async fn renew_preserves_priority() {
        let orig = TxId::new_at(UNIX_EPOCH + std::time::Duration::from_nanos(123_456_789_000));
        let renewed = orig.renew();
        assert_eq!(orig.priority(), renewed.priority());
        assert_ne!(orig, renewed);
        // The fresh random prefix differs from the original.
        assert_ne!(orig.as_bytes()[..8], renewed.as_bytes()[..8]);
    }

    #[tokio::test]
    async fn new_random_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id = TxId::new_random();
            assert!(seen.insert(id), "duplicate transaction identity generated");
        }
    }
}
