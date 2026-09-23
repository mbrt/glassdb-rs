//! Configurable limits on transaction admission.

use crate::Error;

/// Local admission limits applied by one database instance.
///
/// Key and value byte limits apply to write requests. Zero permits only empty
/// keys or values in those requests. The shared node sizing policy can further
/// restrict keys.
#[derive(Debug, Clone, Copy)]
pub struct TransactionLimits {
    /// Maximum logical-key bytes in a write request, including overwrites.
    /// Defaults to 4 KiB. Reads, deletions, and scans do not apply this limit.
    pub max_key_bytes: usize,
    /// Maximum bytes in a value staged for writing. Defaults to 1 MiB.
    /// Existing stored values remain readable regardless of this limit.
    pub max_value_bytes: usize,
    /// Maximum point accesses, scans, and collection operations per execution
    /// of a transaction body. Defaults to 4,096. Repeated calls, including
    /// failed or cancelled operations, count again. Each collection-path
    /// segment counts as one operation. Zero rejects all these operations.
    pub max_operations: usize,
    /// Maximum total key and value bytes submitted by accepted writes and
    /// deletes per transaction-body execution. Defaults to 64 MiB. Replacements
    /// count again; deleting a key counts its bytes and does not refund earlier
    /// writes. This does not include retained read or scan observations.
    pub max_write_bytes: usize,
    /// Maximum new collection bindings reserved by one transaction identity.
    /// Defaults to 1,024. Reservations survive body replays and are not refunded
    /// when a staged collection is dropped. Existing bindings remain usable.
    pub max_collection_reservations: usize,
}

impl TransactionLimits {
    pub(crate) fn check_write_key(&self, key: &[u8]) -> Result<(), Error> {
        check_limit("logical key bytes", key.len(), self.max_key_bytes)
    }

    pub(crate) fn check_value(&self, value: &[u8]) -> Result<(), Error> {
        check_limit("value bytes", value.len(), self.max_value_bytes)
    }
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 4 * 1024,
            max_value_bytes: 1024 * 1024,
            max_operations: 4096,
            max_write_bytes: 64 * 1024 * 1024,
            max_collection_reservations: 1024,
        }
    }
}

fn check_limit(resource: &'static str, size: usize, limit: usize) -> Result<(), Error> {
    if size > limit {
        return Err(Error::LimitExceeded { resource, limit });
    }
    Ok(())
}
