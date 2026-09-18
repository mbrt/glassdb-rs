//! Persistence codec for structural intents.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::{CollectionAddress, NodeToken, TxId};

use crate::error::StorageError;

/// The durable progress of a split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitIntentPhase {
    /// Child identities are reserved, but the split cannot publish yet.
    Preparing,
    /// The source revision is fixed; the split may have created its new nodes.
    Ready,
}

/// The durable progress of a leaf merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeIntentPhase {
    /// Sources are selected, but the merge cannot publish yet.
    Preparing,
    /// Both source revisions are fixed.
    Ready,
    /// The left leaf contains the union; recovery must finish the right redirect.
    Applying,
    /// Left publication is fenced; recovery must release and fence the right gate.
    Aborting,
}

/// The state needed to resolve an interrupted split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitIntent {
    pub collection: CollectionAddress,
    pub participant_id: TxId,
    pub source_token: Option<NodeToken>,
    pub source_version: String,
    pub created_tokens: Vec<NodeToken>,
    pub split_key: Vec<u8>,
    pub phase: SplitIntentPhase,
}

/// The state needed to resolve an interrupted leaf merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeIntent {
    pub collection: CollectionAddress,
    pub participant_id: TxId,
    pub left_token: NodeToken,
    pub left_version: String,
    pub right_token: NodeToken,
    pub right_version: String,
    pub phase: MergeIntentPhase,
}

/// The structural state needed to resolve an interrupted topology change.
///
/// An intent identity is never reused. Recorded source identities and revisions
/// remain fixed after Ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StructuralIntent {
    Split(SplitIntent),
    Merge(MergeIntent),
}

impl SplitIntent {
    /// Reports whether this intent splits the collection root.
    pub fn is_root(&self) -> bool {
        self.source_token.is_none()
    }
}

impl StructuralIntent {
    /// Returns the collection whose topology changes.
    pub fn collection(&self) -> &CollectionAddress {
        match self {
            Self::Split(intent) => &intent.collection,
            Self::Merge(intent) => &intent.collection,
        }
    }

    /// Returns the topology participant that owns this intent.
    pub fn participant_id(&self) -> &TxId {
        match self {
            Self::Split(intent) => &intent.participant_id,
            Self::Merge(intent) => &intent.participant_id,
        }
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
        use pb::structural_intent::Operation;
        let operation = match self {
            Self::Split(intent) => Operation::Split(pb::SplitIntent {
                source_token: intent
                    .source_token
                    .as_ref()
                    .map_or_else(String::new, ToString::to_string),
                source_version: intent.source_version.clone(),
                created_tokens: intent
                    .created_tokens
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
                split_key: intent.split_key.clone(),
                phase: match intent.phase {
                    SplitIntentPhase::Preparing => pb::split_intent::Phase::Preparing.into(),
                    SplitIntentPhase::Ready => pb::split_intent::Phase::Ready.into(),
                },
            }),
            Self::Merge(intent) => Operation::Merge(pb::MergeIntent {
                left_token: intent.left_token.to_string(),
                left_version: intent.left_version.clone(),
                right_token: intent.right_token.to_string(),
                right_version: intent.right_version.clone(),
                phase: match intent.phase {
                    MergeIntentPhase::Preparing => pb::merge_intent::Phase::Preparing.into(),
                    MergeIntentPhase::Ready => pb::merge_intent::Phase::Ready.into(),
                    MergeIntentPhase::Applying => pb::merge_intent::Phase::Applying.into(),
                    MergeIntentPhase::Aborting => pb::merge_intent::Phase::Aborting.into(),
                },
            }),
        };
        pb::StructuralIntent {
            prefix: self.collection().physical_prefix(),
            participant_id: self.participant_id().as_bytes().to_vec(),
            operation: Some(operation),
        }
    }

    fn from_proto(raw: pb::StructuralIntent) -> Result<Self, StorageError> {
        use pb::structural_intent::Operation;
        let participant_id = TxId::from_bytes(raw.participant_id);
        if participant_id.is_unset() {
            return Err(StorageError::other(
                "structural intent has no topology participant",
            ));
        }
        let collection = CollectionAddress::from_physical_prefix(&raw.prefix).map_err(|error| {
            StorageError::with_source("parsing structural-intent collection", error)
        })?;
        match raw.operation {
            Some(Operation::Split(split)) => {
                let phase = match pb::split_intent::Phase::try_from(split.phase) {
                    Ok(pb::split_intent::Phase::Preparing) => SplitIntentPhase::Preparing,
                    Ok(pb::split_intent::Phase::Ready) => SplitIntentPhase::Ready,
                    Err(_) => return Err(StorageError::other("split intent has an invalid phase")),
                };
                let source_token = if split.source_token.is_empty() {
                    None
                } else {
                    Some(NodeToken::try_from(split.source_token).map_err(|error| {
                        StorageError::with_source("parsing split source token", error)
                    })?)
                };
                let created_tokens = split
                    .created_tokens
                    .into_iter()
                    .map(|token| {
                        NodeToken::try_from(token).map_err(|error| {
                            StorageError::with_source("parsing split created token", error)
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::Split(SplitIntent {
                    collection,
                    participant_id,
                    source_token,
                    source_version: split.source_version,
                    created_tokens,
                    split_key: split.split_key,
                    phase,
                }))
            }
            Some(Operation::Merge(merge)) => {
                let phase = match pb::merge_intent::Phase::try_from(merge.phase) {
                    Ok(pb::merge_intent::Phase::Preparing) => MergeIntentPhase::Preparing,
                    Ok(pb::merge_intent::Phase::Ready) => MergeIntentPhase::Ready,
                    Ok(pb::merge_intent::Phase::Applying) => MergeIntentPhase::Applying,
                    Ok(pb::merge_intent::Phase::Aborting) => MergeIntentPhase::Aborting,
                    Err(_) => return Err(StorageError::other("merge intent has an invalid phase")),
                };
                let left_token = NodeToken::try_from(merge.left_token).map_err(|error| {
                    StorageError::with_source("parsing merge left source token", error)
                })?;
                let right_token = NodeToken::try_from(merge.right_token).map_err(|error| {
                    StorageError::with_source("parsing merge right source token", error)
                })?;
                if left_token == right_token {
                    return Err(StorageError::other("merge intent has identical sources"));
                }
                if phase != MergeIntentPhase::Preparing
                    && (merge.left_version.is_empty() || merge.right_version.is_empty())
                {
                    return Err(StorageError::other("merge intent has no source revision"));
                }
                Ok(Self::Merge(MergeIntent {
                    collection,
                    participant_id,
                    left_token,
                    left_version: merge.left_version,
                    right_token,
                    right_version: merge.right_version,
                    phase,
                }))
            }
            None => Err(StorageError::other("structural intent has no operation")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_data::{CollectionId, DbRoot};

    fn collection() -> CollectionAddress {
        CollectionAddress::from_db_root(DbRoot::try_from("db").unwrap(), CollectionId::root())
    }

    fn split_intent() -> SplitIntent {
        SplitIntent {
            collection: collection(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            source_token: Some(NodeToken::from_bytes([1; 16])),
            source_version: "v7".to_string(),
            created_tokens: vec![NodeToken::from_bytes([2; 16])],
            split_key: b"m".to_vec(),
            phase: SplitIntentPhase::Ready,
        }
    }

    fn merge_intent() -> MergeIntent {
        MergeIntent {
            collection: collection(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            left_token: NodeToken::from_bytes([1; 16]),
            left_version: "left-v1".to_string(),
            right_token: NodeToken::from_bytes([2; 16]),
            right_version: "right-v2".to_string(),
            phase: MergeIntentPhase::Ready,
        }
    }

    #[test]
    fn split_intents_round_trip_in_each_phase() {
        for phase in [SplitIntentPhase::Preparing, SplitIntentPhase::Ready] {
            for is_root in [false, true] {
                let mut split = split_intent();
                split.phase = phase;
                if is_root {
                    split.source_token = None;
                    split.created_tokens.push(NodeToken::from_bytes([3; 16]));
                }
                if phase == SplitIntentPhase::Preparing {
                    split.source_version.clear();
                    split.split_key.clear();
                }
                let intent = StructuralIntent::Split(split);
                assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
            }
        }
    }

    #[test]
    fn merge_intents_round_trip_in_each_phase() {
        for phase in [
            MergeIntentPhase::Preparing,
            MergeIntentPhase::Ready,
            MergeIntentPhase::Applying,
            MergeIntentPhase::Aborting,
        ] {
            let mut merge = merge_intent();
            merge.phase = phase;
            if phase == MergeIntentPhase::Preparing {
                merge.left_version.clear();
                merge.right_version.clear();
            }
            let intent = StructuralIntent::Merge(merge);
            assert_eq!(StructuralIntent::decode(&intent.encode()).unwrap(), intent);
        }
    }

    #[test]
    fn merge_intent_rejects_invalid_sources() {
        for (left, right) in [
            ("".to_string(), NodeToken::from_bytes([2; 16]).to_string()),
            (NodeToken::from_bytes([1; 16]).to_string(), "".to_string()),
            (
                "invalid/token".to_string(),
                NodeToken::from_bytes([2; 16]).to_string(),
            ),
            (
                NodeToken::from_bytes([1; 16]).to_string(),
                "invalid/token".to_string(),
            ),
            (
                NodeToken::from_bytes([1; 16]).to_string(),
                NodeToken::from_bytes([1; 16]).to_string(),
            ),
        ] {
            let mut raw = StructuralIntent::Merge(merge_intent()).to_proto();
            let Some(pb::structural_intent::Operation::Merge(merge)) = &mut raw.operation else {
                unreachable!();
            };
            merge.left_token = left;
            merge.right_token = right;
            assert!(StructuralIntent::decode(&raw.encode_to_vec()).is_err());
        }
    }

    #[test]
    fn merge_phases_require_both_source_revisions() {
        for phase in [
            MergeIntentPhase::Ready,
            MergeIntentPhase::Applying,
            MergeIntentPhase::Aborting,
        ] {
            let mut no_left = merge_intent();
            no_left.phase = phase;
            no_left.left_version.clear();
            assert!(StructuralIntent::decode(&StructuralIntent::Merge(no_left).encode()).is_err());
            let mut no_right = merge_intent();
            no_right.phase = phase;
            no_right.right_version.clear();
            assert!(StructuralIntent::decode(&StructuralIntent::Merge(no_right).encode()).is_err());
        }
    }

    #[test]
    fn intents_reject_invalid_phases() {
        for phase in [2, 3, -1, 99] {
            let mut raw = StructuralIntent::Split(split_intent()).to_proto();
            let Some(pb::structural_intent::Operation::Split(split)) = &mut raw.operation else {
                unreachable!();
            };
            split.phase = phase;
            assert!(StructuralIntent::decode(&raw.encode_to_vec()).is_err());
        }
        let mut raw = StructuralIntent::Merge(merge_intent()).to_proto();
        let Some(pb::structural_intent::Operation::Merge(merge)) = &mut raw.operation else {
            unreachable!();
        };
        merge.phase = 99;
        assert!(StructuralIntent::decode(&raw.encode_to_vec()).is_err());
    }

    #[test]
    fn intents_require_an_operation_and_participant() {
        let mut raw = StructuralIntent::Split(split_intent()).to_proto();
        raw.operation = None;
        assert!(StructuralIntent::decode(&raw.encode_to_vec()).is_err());
        let mut raw = StructuralIntent::Split(split_intent()).to_proto();
        raw.participant_id.clear();
        assert!(StructuralIntent::decode(&raw.encode_to_vec()).is_err());
    }

    #[test]
    fn v4_split_intent_encoding() {
        let intent = StructuralIntent::Split(SplitIntent {
            collection: collection(),
            participant_id: TxId::from_bytes(b"participant".to_vec()),
            source_token: None,
            source_version: String::new(),
            created_tokens: vec![
                NodeToken::from_bytes([0; 16]),
                NodeToken::from_bytes([1; 16]),
            ],
            split_key: Vec::new(),
            phase: SplitIntentPhase::Preparing,
        });
        let bytes = [
            b"\x0a\x1c".as_slice(),
            b"db/_c/0000000000000000000000",
            b"\x12\x0bparticipant\x1a\x30\x1a\x16",
            b"0000000000000000000000",
            b"\x1a\x16",
            b"0F410F410F410F410F410F",
        ]
        .concat();
        assert_eq!(intent.encode(), bytes);
        assert_eq!(StructuralIntent::decode(&bytes).unwrap(), intent);
    }
}
