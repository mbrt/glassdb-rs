//! The start value of a node sequence and a record sequence (ADR-109).

use glassdb_concurr::entropy::fill_bytes;

/// Returns a random start value for the sequence of a newly created object.
///
/// Two create requests for the same path then write different states, so a
/// CAS that expected the state of one create cannot land on the state of
/// another one, also after a delete between them.
pub(crate) fn random_start() -> u32 {
    let mut bytes = [0; 4];
    fill_bytes(&mut bytes);
    u32::from_le_bytes(bytes)
}
