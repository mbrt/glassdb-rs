//! Persistence codec for structural intents.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::{CollectionAddress, NodeToken, TxId};

use crate::error::StorageError;

/// Whether a split intent has captured the source version needed by recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralIntentPhase {
    /// Tokens are reserved, but no structural node may have been created yet.
    Preparing,
    /// The source is gated and node creation may have started.
    Ready,
}

/// The structural state needed to resolve a crash-interrupted split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralIntent {
    pub collection: CollectionAddress,
    pub source_token: Option<NodeToken>,
    pub source_version: String,
    pub created_tokens: Vec<NodeToken>,
    pub split_key: Vec<u8>,
    pub participant_id: TxId,
    pub phase: StructuralIntentPhase,
}

impl StructuralIntent {
    /// Reports whether this intent splits the collection root.
    pub fn is_root(&self) -> bool {
        self.source_token.is_none()
    }

    /// Encodes this structural intent for storage under `_s`.
    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Decodes a structural intent stored under `_s`.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::StructuralIntent::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling structural intent", e))?;
        Self::from_proto(raw)
    }

    fn to_proto(&self) -> pb::StructuralIntent {
        pb::StructuralIntent {
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
                StructuralIntentPhase::Preparing => pb::structural_intent::Phase::Preparing.into(),
                StructuralIntentPhase::Ready => pb::structural_intent::Phase::Ready.into(),
            },
        }
    }

    fn from_proto(raw: pb::StructuralIntent) -> Result<Self, StorageError> {
        let participant_id = TxId::from_bytes(raw.participant_id);
        if participant_id.is_unset() {
            return Err(StorageError::other(
                "structural intent has no topology participant",
            ));
        }
        let phase = match pb::structural_intent::Phase::try_from(raw.phase) {
            Ok(pb::structural_intent::Phase::Preparing) => StructuralIntentPhase::Preparing,
            Ok(pb::structural_intent::Phase::Ready) => StructuralIntentPhase::Ready,
            Err(_) => {
                return Err(StorageError::other(
                    "structural intent has an invalid phase",
                ));
            }
        };
        let collection = CollectionAddress::from_physical_prefix(&raw.prefix).map_err(|error| {
            StorageError::with_source("parsing structural-intent collection", error)
        })?;
        let source_token = if raw.source_token.is_empty() {
            None
        } else {
            Some(NodeToken::try_from(raw.source_token).map_err(|error| {
                StorageError::with_source("parsing structural-intent source token", error)
            })?)
        };
        if raw.is_root != source_token.is_none() {
            return Err(StorageError::other(
                "structural intent has inconsistent root metadata",
            ));
        }
        let created_tokens = raw
            .created_tokens
            .into_iter()
            .map(|token| {
                NodeToken::try_from(token).map_err(|error| {
                    StorageError::with_source("parsing structural-intent created token", error)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StructuralIntent {
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
    fn intent_round_trips() {
        let intent = StructuralIntent {
            collection: collection(),
            source_token: Some(NodeToken::from_bytes([1; 16])),
            source_version: "v7".to_string(),
            created_tokens: vec![NodeToken::from_bytes([2; 16])],
            split_key: b"m".to_vec(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Ready,
        };
        assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
    }

    #[test]
    fn root_intent_round_trips() {
        let intent = StructuralIntent {
            collection: collection(),
            source_token: None,
            source_version: "v1".to_string(),
            created_tokens: vec![
                NodeToken::from_bytes([1; 16]),
                NodeToken::from_bytes([2; 16]),
            ],
            split_key: Vec::new(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Preparing,
        };
        assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
    }

    #[test]
    fn pre_rename_structural_intent_bytes_remain_compatible() {
        // The vocabulary change must not change the persisted protobuf wire format.
        let intent = StructuralIntent {
            collection: collection(),
            source_token: None,
            source_version: "v1".to_string(),
            created_tokens: vec![NodeToken::from_bytes([0; 16])],
            split_key: Vec::new(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Preparing,
        };
        let bytes = [
            b"\x0a\x1c".as_slice(),
            b"db/_c/0000000000000000000000",
            b"\x1a\x02v1\x22\x16",
            b"0000000000000000000000",
            b"\x30\x01\x3a\x0bparticipant",
        ]
        .concat();

        assert_eq!(intent.encode(), bytes);
        assert_eq!(StructuralIntent::decode(&bytes).unwrap(), intent);
    }
}
