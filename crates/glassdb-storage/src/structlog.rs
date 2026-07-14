//! Persistence codec for split write-ahead records.

use glassdb_proto as pb;
use prost::Message;

use crate::error::StorageError;

/// The structural state needed to resolve a crash-interrupted split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralLog {
    pub prefix: String,
    pub source_token: String,
    pub source_version: String,
    pub created_tokens: Vec<String>,
    pub split_key: Vec<u8>,
    pub is_root: bool,
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
        Ok(Self::from_proto(raw))
    }

    fn to_proto(&self) -> pb::StructuralLog {
        pb::StructuralLog {
            prefix: self.prefix.clone(),
            source_token: self.source_token.clone(),
            source_version: self.source_version.clone(),
            created_tokens: self.created_tokens.clone(),
            split_key: self.split_key.clone(),
            is_root: self.is_root,
        }
    }

    fn from_proto(raw: pb::StructuralLog) -> Self {
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
    fn record_round_trips() {
        let record = StructuralLog {
            prefix: "db/coll".to_string(),
            source_token: "left".to_string(),
            source_version: "v7".to_string(),
            created_tokens: vec!["right".to_string()],
            split_key: b"m".to_vec(),
            is_root: false,
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
        };
        assert_eq!(StructuralLog::decode(&record.encode()).unwrap(), record);
    }
}
