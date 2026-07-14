//! The split write-ahead structural-log record (ADR-032).
//!
//! Before a split creates any node object it durably writes one of these
//! records, so a crash mid-split leaves a discoverable note. Recovery resolves
//! the split from **structural** state — the tree-reachability of the created
//! nodes — never transaction status, because a split's shrink CAS is not atomic
//! with any status transition. The record replaces ADR-031's `_g/`
//! split-active registry and its generational orphan sweep.

use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;

/// A split's write-ahead structural-log record (ADR-032). Names the source it
/// shrinks (a `_n` node or the root `_i`), the version its shrink CAS is
/// guarded by, the freshly created node token(s), and the separator to publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralLog {
    /// The collection prefix the split runs in (e.g. `db/coll`).
    pub prefix: String,
    /// The source node token the split shrinks; empty for an in-place root split.
    pub source_token: String,
    /// The source object's CAS version token at the time the record was written.
    pub source_version: String,
    /// The freshly created node token(s): the right sibling for a leaf/interior
    /// split, or the two children for a root split.
    pub created_tokens: Vec<String>,
    /// The split key (the separator to publish into the parent); empty for a
    /// root split.
    pub split_key: Vec<u8>,
    /// Whether the source is the collection root `_i` (an in-place root split).
    pub is_root: bool,
}

impl StructuralLog {
    /// Encodes the record to its canonical protobuf body (the object payload).
    pub fn encode(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    /// Decodes a record from its protobuf body.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::StructuralLog::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling structural log", e))?;
        Ok(StructuralLog::from_pb(raw))
    }

    fn to_pb(&self) -> pb::StructuralLog {
        pb::StructuralLog {
            prefix: self.prefix.clone(),
            source_token: self.source_token.clone(),
            source_version: self.source_version.clone(),
            created_tokens: self.created_tokens.clone(),
            split_key: self.split_key.clone(),
            is_root: self.is_root,
        }
    }

    fn from_pb(raw: pb::StructuralLog) -> Self {
        StructuralLog {
            prefix: raw.prefix,
            source_token: raw.source_token,
            source_version: raw.source_version,
            created_tokens: raw.created_tokens,
            split_key: raw.split_key,
            is_root: raw.is_root,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips_through_its_encoding() {
        let rec = StructuralLog {
            prefix: "db/coll".to_string(),
            source_token: "SRC".to_string(),
            source_version: "v7".to_string(),
            created_tokens: vec!["RIGHT".to_string()],
            split_key: b"m".to_vec(),
            is_root: false,
        };
        assert_eq!(StructuralLog::decode(&rec.encode()).unwrap(), rec);
    }

    #[test]
    fn root_record_round_trips() {
        let rec = StructuralLog {
            prefix: "db/coll".to_string(),
            source_token: String::new(),
            source_version: "v1".to_string(),
            created_tokens: vec!["L".to_string(), "R".to_string()],
            split_key: Vec::new(),
            is_root: true,
        };
        assert_eq!(StructuralLog::decode(&rec.encode()).unwrap(), rec);
    }
}
