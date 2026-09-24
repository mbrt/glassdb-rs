//! Persistence codec for structural intents.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::{CollectionAddress, NodeToken, TxId};

use crate::error::StorageError;

/// Whether a structural intent has captured the source revision needed by
/// recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuralIntentPhase {
    /// The change is reserved, but no node except the gated source may have
    /// changed yet.
    Preparing,
    /// The source is gated and other nodes may have started to change.
    Ready,
}

/// The tree change that one structural intent describes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructuralChange {
    /// Moves the upper half of the source into newly created nodes.
    Split {
        created_tokens: Vec<NodeToken>,
        split_key: Vec<u8>,
    },
    /// Moves the range and entries of the source into its right sibling
    /// (ADR-073). The target is known only after the Ready transition.
    Merge { target: Option<MergeTarget> },
}

/// The node that receives a merge, as recorded at the Ready transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeTarget {
    pub token: NodeToken,
    /// The source's high key: the low bound of the target before the merge.
    pub boundary: Vec<u8>,
    /// The membership generation of the target at Ready. The absorb lands only
    /// while the target has it, so that recovery can fence a late absorb by
    /// advancing it.
    pub generation: u64,
}

/// The structural state needed to resolve a crash-interrupted split or merge.
///
/// An intent identity is never reused. Its only update is Preparing to Ready;
/// a Ready body stays fixed until deletion. Recovery relies on these rules
/// when it accepts a cached body as a discovery candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuralIntent {
    pub collection: CollectionAddress,
    pub source_token: Option<NodeToken>,
    pub source_revision: String,
    pub change: StructuralChange,
    pub participant_id: TxId,
    pub phase: StructuralIntentPhase,
}

impl StructuralIntent {
    /// Reports whether this intent splits the tree root.
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
        let (created_tokens, split_key, merge) = match &self.change {
            StructuralChange::Split {
                created_tokens,
                split_key,
            } => (
                created_tokens.iter().map(ToString::to_string).collect(),
                split_key.clone(),
                None,
            ),
            StructuralChange::Merge { target } => (
                Vec::new(),
                Vec::new(),
                Some(pb::MergeIntent {
                    target_token: target
                        .as_ref()
                        .map_or_else(String::new, |target| target.token.to_string()),
                    boundary: target
                        .as_ref()
                        .map(|target| target.boundary.clone())
                        .unwrap_or_default(),
                    target_generation: target.as_ref().map_or(0, |target| target.generation),
                }),
            ),
        };
        pb::StructuralIntent {
            prefix: self.collection.physical_prefix(),
            source_token: self
                .source_token
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            source_revision: self.source_revision.clone(),
            created_tokens,
            split_key,
            is_root: self.is_root(),
            participant_id: self.participant_id.as_bytes().to_vec(),
            phase: match self.phase {
                StructuralIntentPhase::Preparing => pb::structural_intent::Phase::Preparing.into(),
                StructuralIntentPhase::Ready => pb::structural_intent::Phase::Ready.into(),
            },
            merge,
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
        let change = match raw.merge {
            Some(merge) => Self::merge_from_proto(
                merge,
                phase,
                source_token.is_none(),
                raw.created_tokens.is_empty() && raw.split_key.is_empty(),
            )?,
            None => StructuralChange::Split {
                created_tokens: raw
                    .created_tokens
                    .into_iter()
                    .map(|token| {
                        NodeToken::try_from(token).map_err(|error| {
                            StorageError::with_source(
                                "parsing structural-intent created token",
                                error,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                split_key: raw.split_key,
            },
        };
        Ok(StructuralIntent {
            collection,
            source_token,
            source_revision: raw.source_revision,
            change,
            participant_id,
            phase,
        })
    }

    fn merge_from_proto(
        raw: pb::MergeIntent,
        phase: StructuralIntentPhase,
        is_root: bool,
        has_no_split_fields: bool,
    ) -> Result<StructuralChange, StorageError> {
        if is_root || !has_no_split_fields {
            return Err(StorageError::other(
                "merge intent must have a non-root source and no split fields",
            ));
        }
        let ready = phase == StructuralIntentPhase::Ready;
        if raw.target_token.is_empty() == ready
            || raw.boundary.is_empty() == ready
            || (!ready && raw.target_generation != 0)
        {
            return Err(StorageError::other(
                "merge intent has a target exactly when it is Ready",
            ));
        }
        let target = if ready {
            Some(MergeTarget {
                token: NodeToken::try_from(raw.target_token).map_err(|error| {
                    StorageError::with_source("parsing merge-intent target token", error)
                })?,
                boundary: raw.boundary,
                generation: raw.target_generation,
            })
        } else {
            None
        };
        Ok(StructuralChange::Merge { target })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_data::{CollectionId, DbPrefix};

    fn collection() -> CollectionAddress {
        CollectionAddress::from_db_prefix(DbPrefix::try_from("db").unwrap(), CollectionId::root())
    }

    #[test]
    fn intent_round_trips() {
        let intent = StructuralIntent {
            collection: collection(),
            source_token: Some(NodeToken::from_bytes([1; 16])),
            source_revision: "v7".to_string(),
            change: StructuralChange::Split {
                created_tokens: vec![NodeToken::from_bytes([2; 16])],
                split_key: b"m".to_vec(),
            },
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
            source_revision: "v1".to_string(),
            change: StructuralChange::Split {
                created_tokens: vec![
                    NodeToken::from_bytes([1; 16]),
                    NodeToken::from_bytes([2; 16]),
                ],
                split_key: Vec::new(),
            },
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Preparing,
        };
        assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
    }

    #[test]
    fn merge_intents_round_trip_in_both_phases() {
        let mut intent = StructuralIntent {
            collection: collection(),
            source_token: Some(NodeToken::from_bytes([1; 16])),
            source_revision: String::new(),
            change: StructuralChange::Merge { target: None },
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Preparing,
        };
        assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);

        intent.source_revision = "v3".to_string();
        intent.change = StructuralChange::Merge {
            target: Some(MergeTarget {
                token: NodeToken::from_bytes([2; 16]),
                boundary: b"m".to_vec(),
                generation: 4,
            }),
        };
        intent.phase = StructuralIntentPhase::Ready;
        assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
    }

    #[test]
    fn decode_rejects_inconsistent_merge_intents() {
        let valid = pb::StructuralIntent {
            prefix: collection().physical_prefix(),
            source_token: NodeToken::from_bytes([1; 16]).to_string(),
            participant_id: b"participant".to_vec(),
            phase: pb::structural_intent::Phase::Ready.into(),
            merge: Some(pb::MergeIntent {
                target_token: NodeToken::from_bytes([2; 16]).to_string(),
                boundary: b"m".to_vec(),
                target_generation: 4,
            }),
            ..pb::StructuralIntent::default()
        };
        assert!(StructuralIntent::decode(&valid.encode_to_vec()).is_ok());

        type IntentEdit = fn(&mut pb::StructuralIntent);
        let cases: [(&str, IntentEdit); 6] = [
            ("root source", |raw| {
                raw.source_token.clear();
                raw.is_root = true;
            }),
            ("created token", |raw| {
                raw.created_tokens = vec![NodeToken::from_bytes([3; 16]).to_string()];
            }),
            ("Ready without target", |raw| {
                raw.merge = Some(pb::MergeIntent::default());
            }),
            ("Ready without boundary", |raw| {
                raw.merge.as_mut().unwrap().boundary.clear();
            }),
            ("Preparing with target", |raw| {
                raw.phase = pb::structural_intent::Phase::Preparing.into();
            }),
            ("Preparing with target generation", |raw| {
                raw.phase = pb::structural_intent::Phase::Preparing.into();
                raw.merge = Some(pb::MergeIntent {
                    target_generation: 1,
                    ..pb::MergeIntent::default()
                });
            }),
        ];
        for (name, edit) in cases {
            let mut raw = valid.clone();
            edit(&mut raw);
            assert!(
                StructuralIntent::decode(&raw.encode_to_vec()).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn golden_merge_intent_encoding() {
        let intent = StructuralIntent {
            collection: collection(),
            source_token: Some(NodeToken::from_bytes([0; 16])),
            source_revision: "v1".to_string(),
            change: StructuralChange::Merge {
                target: Some(MergeTarget {
                    token: NodeToken::from_bytes([0; 16]),
                    boundary: b"m".to_vec(),
                    generation: 5,
                }),
            },
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            phase: StructuralIntentPhase::Ready,
        };
        let bytes = [
            b"\x0a\x1c".as_slice(),
            b"db/_c/0000000000000000000000",
            b"\x12\x16",
            b"0000000000000000000000",
            b"\x1a\x02v1\x3a\x0bparticipant\x40\x01\x4a\x1d\x0a\x16",
            b"0000000000000000000000",
            b"\x12\x01m\x18\x05",
        ]
        .concat();

        assert_eq!(intent.encode(), bytes);
        assert_eq!(StructuralIntent::decode(&bytes).unwrap(), intent);
    }

    #[test]
    fn pre_rename_structural_intent_bytes_remain_compatible() {
        // The vocabulary change must not change the persisted protobuf wire format.
        let intent = StructuralIntent {
            collection: collection(),
            source_token: None,
            source_revision: "v1".to_string(),
            change: StructuralChange::Split {
                created_tokens: vec![NodeToken::from_bytes([0; 16])],
                split_key: Vec::new(),
            },
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
