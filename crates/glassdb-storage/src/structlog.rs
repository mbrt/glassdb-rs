//! Persistence codec for split write-ahead records.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::TxId;

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
    pub prefix: String,
    pub source_token: String,
    pub source_version: String,
    pub created_tokens: Vec<String>,
    pub split_key: Vec<u8>,
    pub is_root: bool,
    pub participant_id: TxId,
    pub phase: StructuralLogPhase,
}

impl StructuralLog {
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
            prefix: self.prefix.clone(),
            source_token: self.source_token.clone(),
            source_version: self.source_version.clone(),
            created_tokens: self.created_tokens.clone(),
            split_key: self.split_key.clone(),
            is_root: self.is_root,
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
        Ok(StructuralLog {
            prefix: raw.prefix,
            source_token: raw.source_token,
            source_version: raw.source_version,
            created_tokens: raw.created_tokens,
            split_key: raw.split_key,
            is_root: raw.is_root,
            participant_id,
            phase,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trips() {
        let record = StructuralLog {
            prefix: "db/coll".to_string(),
            source_token: "left".to_string(),
            source_version: "v7".to_string(),
            created_tokens: vec!["right".to_string()],
            split_key: b"m".to_vec(),
            is_root: false,
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralLogPhase::Ready,
        };
        assert_eq!(StructuralLog::decode(&record.encode()).unwrap(), record);
    }

    #[test]
    fn root_record_round_trips() {
        let record = StructuralLog {
            prefix: "db/coll".to_string(),
            source_token: String::new(),
            source_version: "v1".to_string(),
            created_tokens: vec!["left".to_string(), "right".to_string()],
            split_key: Vec::new(),
            is_root: true,
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralLogPhase::Preparing,
        };
        assert_eq!(StructuralLog::decode(&record.encode()).unwrap(), record);
    }
}
