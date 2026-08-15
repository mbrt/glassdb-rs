use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use glassdb_concurr::entropy::fill_bytes;

/// Offset of the big-endian UnixNano timestamp within the ID.
const TX_ID_TS_OFF: usize = 8;

/// A transaction identifier. Ported from the Go `data.TxID` (`[]byte`).
///
/// The layout is `[8 bytes random][8 bytes big-endian UnixNano timestamp]`. The
/// random bytes come first so that transaction-log keys keep a high-entropy
/// prefix, spreading writes across object-storage partitions instead of
/// clustering sequential commits into a single hot partition. The timestamp
/// suffix encodes the transaction priority used by the wound-wait rule: an
/// earlier timestamp means an older, higher-priority transaction.
///
/// A `TxId` can also hold an arbitrary byte sequence (e.g. when decoded from a
/// storage tag).
///
/// [`Ord`] compares the complete ID bytes for identity-keyed collections and is
/// not wound-wait priority order; use [`TxId::older`] for priority decisions.
///
/// The bytes are stored behind an `Arc` so that cloning an id - which happens
/// pervasively (lockers, last-writer versions, cache entries, every commit) -
/// is a refcount bump rather than a heap allocation and copy.
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxId(Arc<[u8]>);

impl TxId {
    /// Maximum number of protobuf bytes used by an ID minted by GlassDB.
    ///
    /// [`TxId::from_bytes`] deliberately preserves arbitrary persisted IDs, so
    /// this is a bound on generated and renewed IDs rather than every value the
    /// compatibility wrapper can hold.
    pub const MAX_GENERATED_ENCODED_LEN: usize = 16;

    /// Generates a new random 128-bit transaction ID.
    ///
    /// The timestamp suffix is random rather than clock-derived: this keeps the
    /// `data` crate free of any clock dependency. Production code mints IDs via
    /// [`TxId::new_at`] with a clock-sourced timestamp; this constructor is for
    /// callers (mostly tests) that only need a unique identifier.
    pub fn new_random() -> Self {
        let mut b = vec![0u8; Self::MAX_GENERATED_ENCODED_LEN];
        fill_bytes(&mut b);
        TxId(b.into())
    }

    /// Builds a transaction ID from a random prefix and an explicit instant,
    /// whose UnixNano timestamp determines the wound-wait priority. The caller
    /// supplies the instant (typically the monitor's clock), so the `data` crate
    /// never sources time itself. Instants at or before the Unix epoch saturate
    /// to priority zero (the highest priority).
    pub fn new_at(t: SystemTime) -> Self {
        let unix_nanos = t
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut b = vec![0u8; Self::MAX_GENERATED_ENCODED_LEN];
        fill_bytes(&mut b[..TX_ID_TS_OFF]);
        b[TX_ID_TS_OFF..].copy_from_slice(&unix_nanos.to_be_bytes());
        TxId(b.into())
    }

    /// Builds a transaction ID from an explicit timestamp and random prefix.
    /// Meant for tests that need deterministic priorities. At most the first 8
    /// bytes of `prefix` are used.
    pub fn with_priority(unix_nanos: u64, prefix: &[u8]) -> Self {
        let mut b = vec![0u8; Self::MAX_GENERATED_ENCODED_LEN];
        let n = prefix.len().min(TX_ID_TS_OFF);
        b[..n].copy_from_slice(&prefix[..n]);
        b[TX_ID_TS_OFF..].copy_from_slice(&unix_nanos.to_be_bytes());
        TxId(b.into())
    }

    /// Returns a transaction ID that preserves the priority (timestamp) of
    /// `self` but uses a fresh random prefix. A wounded transaction reuses its
    /// priority on restart to avoid starvation, while the new prefix gives it a
    /// distinct log object that lands in a different storage partition.
    pub fn renew(&self) -> Self {
        let mut b = vec![0u8; Self::MAX_GENERATED_ENCODED_LEN];
        fill_bytes(&mut b[..TX_ID_TS_OFF]);
        b[TX_ID_TS_OFF..].copy_from_slice(&self.priority().to_be_bytes());
        TxId(b.into())
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
    /// other; that rare tie is left to the serial-locking deadlock safety net.
    pub fn older(&self, other: &TxId) -> bool {
        self.priority() < other.priority()
    }

    /// Wraps raw bytes as a transaction ID.
    pub fn from_bytes(b: impl Into<Vec<u8>>) -> Self {
        let v: Vec<u8> = b.into();
        TxId(v.into())
    }

    /// Returns the raw bytes of the ID.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the ID and returns the owned bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Reports whether the ID is unset.
    pub fn is_unset(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the wound-wait priority (the big-endian UnixNano timestamp
    /// suffix). Defensive on short IDs, which have no timestamp and thus the
    /// highest priority (zero).
    fn priority(&self) -> u64 {
        if self.0.len() < Self::MAX_GENERATED_ENCODED_LEN {
            return 0;
        }
        let mut ts = [0u8; 8];
        ts.copy_from_slice(&self.0[TX_ID_TS_OFF..Self::MAX_GENERATED_ENCODED_LEN]);
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

impl From<Vec<u8>> for TxId {
    fn from(b: Vec<u8>) -> Self {
        TxId(b.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These run on a runtime so prefix minting works under any build; outside
    // the simulation executor the shared entropy facade uses the process RNG.
    #[tokio::test]
    async fn random_is_16_bytes_and_hex() {
        let id = TxId::new_random();
        assert_eq!(id.as_bytes().len(), TxId::MAX_GENERATED_ENCODED_LEN);
        assert_eq!(id.to_string().len(), 32);
    }

    #[tokio::test]
    async fn new_at_layout() {
        let nanos = 1_700_000_000_000_000_000u64;
        let id = TxId::new_at(UNIX_EPOCH + std::time::Duration::from_nanos(nanos));
        assert_eq!(id.as_bytes().len(), 16);
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
        assert_eq!(renewed.as_bytes().len(), 16);
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
            assert!(seen.insert(id.into_bytes()), "duplicate TxID generated");
        }
    }
}
