//! Persistence codec for split write-ahead records.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::{CollectionAddress, NodeToken, TxId};

use crate::error::StorageError;

/// Whether a split intent has captured the source version needed by recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralLogPhase {
    /// Tokens are reserved, but no structural node may have been created yet.
    Preparing,
    /// The source is gated and node creation may have started.
    Ready,
}

/// The structural state needed to resolve a crash-interrupted split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralLog {
    pub collection: CollectionAddress,
    pub source_token: Option<NodeToken>,
    pub source_version: String,
    pub created_tokens: Vec<NodeToken>,
    pub split_key: Vec<u8>,
    pub participant_id: TxId,
    pub phase: StructuralLogPhase,
}

impl StructuralLog {
    /// Reports whether this intent splits the collection root.
    pub fn is_root(&self) -> bool {
        self.source_token.is_none()
    }

    /// Encodes this record for storage under `_s`.
    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Decodes a record stored under `_s`.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::StructuralLog::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling structural log", e))?;
        Self::from_proto(raw)
    }

    fn to_proto(&self) -> pb::StructuralLog {
        pb::StructuralLog {
            prefix: self.collection.physical_prefix(),
            source_token: self
                .source_token
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            source_version: self.source_version.clone(),
            created_tokens: self
                .created_tokens
                .iter()
                .map(ToString::to_string)
                .collect(),
            split_key: self.split_key.clone(),
            is_root: self.is_root(),
            participant_id: self.participant_id.as_bytes().to_vec(),
            phase: match self.phase {
                StructuralLogPhase::Preparing => pb::structural_log::Phase::Preparing.into(),
                StructuralLogPhase::Ready => pb::structural_log::Phase::Ready.into(),
            },
        }
    }

    fn from_proto(raw: pb::StructuralLog) -> Result<Self, StorageError> {
        let participant_id = TxId::from_bytes(raw.participant_id);
        if participant_id.is_unset() {
            return Err(StorageError::other(
                "structural log has no topology participant",
            ));
        }
        let phase = match pb::structural_log::Phase::try_from(raw.phase) {
            Ok(pb::structural_log::Phase::Preparing) => StructuralLogPhase::Preparing,
            Ok(pb::structural_log::Phase::Ready) => StructuralLogPhase::Ready,
            Err(_) => return Err(StorageError::other("structural log has an invalid phase")),
        };
        let collection = CollectionAddress::from_physical_prefix(&raw.prefix).map_err(|error| {
            StorageError::with_source("parsing structural-log collection", error)
        })?;
        let source_token = if raw.source_token.is_empty() {
            None
        } else {
            Some(NodeToken::try_from(raw.source_token).map_err(|error| {
                StorageError::with_source("parsing structural-log source token", error)
            })?)
        };
        if raw.is_root != source_token.is_none() {
            return Err(StorageError::other(
                "structural log has inconsistent root metadata",
            ));
        }
        let created_tokens = raw
            .created_tokens
            .into_iter()
            .map(|token| {
                NodeToken::try_from(token).map_err(|error| {
                    StorageError::with_source("parsing structural-log created token", error)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StructuralLog {
            collection,
            source_token,
            source_version: raw.source_version,
            created_tokens,
            split_key: raw.split_key,
            participant_id,
            phase,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_data::{CollectionId, DbRoot};

    fn collection() -> CollectionAddress {
        CollectionAddress::from_db_root(DbRoot::try_from("db").unwrap(), CollectionId::root())
    }

    #[test]
    fn record_round_trips() {
        let record = StructuralLog {
            collection: collection(),
            source_token: Some(NodeToken::from_bytes([1; 16])),
            source_version: "v7".to_string(),
            created_tokens: vec![NodeToken::from_bytes([2; 16])],
            split_key: b"m".to_vec(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralLogPhase::Ready,
        };
        assert_eq!(StructuralLog::decode(&record.encode()).unwrap(), record);
    }

    #[test]
    fn root_record_round_trips() {
        let record = StructuralLog {
            collection: collection(),
            source_token: None,
            source_version: "v1".to_string(),
            created_tokens: vec![
                NodeToken::from_bytes([1; 16]),
                NodeToken::from_bytes([2; 16]),
            ],
            split_key: Vec::new(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralLogPhase::Preparing,
        };
        assert_eq!(StructuralLog::decode(&record.encode()).unwrap(), record);
    }
}
