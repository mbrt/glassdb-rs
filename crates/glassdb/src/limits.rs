//! Configurable limits on transaction inputs.

use crate::Error;

/// Limits applied by one database instance before it copies transaction inputs.
///
/// These are local admission limits, not stored format limits. A zero byte limit
/// permits only empty inputs. The node sizing policy can further restrict keys.
#[derive(Debug, Clone, Copy)]
pub struct TransactionLimits {
    /// Maximum logical-key bytes, including caller-supplied scan bounds.
    /// Defaults to 4 KiB.
    pub max_key_bytes: usize,
    /// Maximum bytes in a value staged for writing. Defaults to 1 MiB.
    /// Existing stored values remain readable regardless of this limit.
    pub max_value_bytes: usize,
}

impl TransactionLimits {
    pub(crate) fn check_key(&self, key: &[u8]) -> Result<(), Error> {
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
        }
    }
}

fn check_limit(resource: &'static str, size: usize, limit: usize) -> Result<(), Error> {
    if size > limit {
        return Err(Error::LimitExceeded { resource, limit });
    }
    Ok(())
}
