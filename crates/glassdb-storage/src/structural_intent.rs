//! Persistence codec for structural intents.

use glassdb_proto as pb;
use prost::Message;

use glassdb_data::{CollectionAddress, CollectionId, DbPrefix, NodeId, TxId};

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
        created_node_ids: Vec<NodeId>,
        split_key: Vec<u8>,
    },
    /// Moves the range and entries of the source into its right sibling
    /// (ADR-073). The target is known only after the Ready transition.
    Merge { target: Option<MergeTarget> },
}

/// The node that receives a merge, as recorded at the Ready transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeTarget {
    pub node_id: NodeId,
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
    pub source_node_id: Option<NodeId>,
    pub source_revision: String,
    pub change: StructuralChange,
    pub participant_id: TxId,
    pub phase: StructuralIntentPhase,
}

impl StructuralIntent {
    /// Reports whether this intent splits the tree root.
    pub fn is_root(&self) -> bool {
        self.source_node_id.is_none()
    }

    /// Encodes this structural intent for storage under `_s`.
    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Decodes a structural intent stored under `_s` of `db_prefix`.
    pub fn decode(db_prefix: &DbPrefix, buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::StructuralIntent::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling structural intent", e))?;
        Self::from_proto(db_prefix, raw)
    }

    fn to_proto(&self) -> pb::StructuralIntent {
        let (created_node_ids, split_key, merge) = match &self.change {
            StructuralChange::Split {
                created_node_ids,
                split_key,
            } => (
                created_node_ids
                    .iter()
                    .map(|id| id.as_bytes().to_vec())
                    .collect(),
                split_key.clone(),
                None,
            ),
            StructuralChange::Merge { target } => (
                Vec::new(),
                Vec::new(),
                Some(pb::MergeIntent {
                    target_node_id: target
                        .as_ref()
                        .map(|target| target.node_id.as_bytes().to_vec())
                        .unwrap_or_default(),
                    boundary: target
                        .as_ref()
                        .map(|target| target.boundary.clone())
                        .unwrap_or_default(),
                    target_generation: target.as_ref().map_or(0, |target| target.generation),
                }),
            ),
        };
        pb::StructuralIntent {
            collection_id: self.collection.id().as_bytes().to_vec(),
            source_node_id: self
                .source_node_id
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
            source_revision: self.source_revision.clone(),
            created_node_ids,
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

    fn from_proto(db_prefix: &DbPrefix, raw: pb::StructuralIntent) -> Result<Self, StorageError> {
        let participant_id = TxId::from_slice(&raw.participant_id).ok_or_else(|| {
            StorageError::other("structural intent has an invalid topology participant")
        })?;
        let phase = match pb::structural_intent::Phase::try_from(raw.phase) {
            Ok(pb::structural_intent::Phase::Preparing) => StructuralIntentPhase::Preparing,
            Ok(pb::structural_intent::Phase::Ready) => StructuralIntentPhase::Ready,
            Err(_) => {
                return Err(StorageError::other(
                    "structural intent has an invalid phase",
                ));
            }
        };
        let collection_id = CollectionId::from_slice(&raw.collection_id)
            .ok_or_else(|| StorageError::other("structural intent has an invalid collection ID"))?;
        let collection = CollectionAddress::from_db_prefix(db_prefix.clone(), collection_id);
        let source_node_id = if raw.source_node_id.is_empty() {
            None
        } else {
            Some(parse_node_id(&raw.source_node_id, "source")?)
        };
        if raw.is_root != source_node_id.is_none() {
            return Err(StorageError::other(
                "structural intent has inconsistent root metadata",
            ));
        }
        let change = match raw.merge {
            Some(merge) => Self::merge_from_proto(
                merge,
                phase,
                source_node_id.is_none(),
                raw.created_node_ids.is_empty() && raw.split_key.is_empty(),
            )?,
            None => StructuralChange::Split {
                created_node_ids: raw
                    .created_node_ids
                    .iter()
                    .map(|id| parse_node_id(id, "created"))
                    .collect::<Result<Vec<_>, _>>()?,
                split_key: raw.split_key,
            },
        };
        Ok(StructuralIntent {
            collection,
            source_node_id,
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
        if raw.target_node_id.is_empty() == ready
            || raw.boundary.is_empty() == ready
            || (!ready && raw.target_generation != 0)
        {
            return Err(StorageError::other(
                "merge intent has a target exactly when it is Ready",
            ));
        }
        let target = if ready {
            Some(MergeTarget {
                node_id: parse_node_id(&raw.target_node_id, "merge target")?,
                boundary: raw.boundary,
                generation: raw.target_generation,
            })
        } else {
            None
        };
        Ok(StructuralChange::Merge { target })
    }
}

fn parse_node_id(bytes: &[u8], role: &str) -> Result<NodeId, StorageError> {
    NodeId::from_slice(bytes).ok_or_else(|| {
        StorageError::other(format!("structural intent has an invalid {role} node ID"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    fn db_prefix() -> DbPrefix {
        DbPrefix::try_from("db").unwrap()
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::from_db_prefix(db_prefix(), CollectionId::root())
    }

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn decode(raw: &pb::StructuralIntent) -> Result<StructuralIntent, StorageError> {
        StructuralIntent::decode(&db_prefix(), &raw.encode_to_vec())
    }

    fn round_trip(intent: &StructuralIntent) -> StructuralIntent {
        StructuralIntent::decode(&db_prefix(), &intent.encode()).unwrap()
    }

    #[test]
    fn intent_round_trips() {
        let intent = StructuralIntent {
            collection: collection(),
            source_node_id: Some(node_id(1)),
            source_revision: "v7".to_string(),
            change: StructuralChange::Split {
                created_node_ids: vec![node_id(2)],
                split_key: b"m".to_vec(),
            },
            participant_id: tx_id(b"participant"),
            phase: StructuralIntentPhase::Ready,
        };
        assert_eq!(round_trip(&intent), intent);
    }

    #[test]
    fn root_intent_round_trips() {
        let intent = StructuralIntent {
            collection: collection(),
            source_node_id: None,
            source_revision: "v1".to_string(),
            change: StructuralChange::Split {
                created_node_ids: vec![node_id(1), node_id(2)],
                split_key: Vec::new(),
            },
            participant_id: tx_id(b"participant"),
            phase: StructuralIntentPhase::Preparing,
        };
        assert_eq!(round_trip(&intent), intent);
    }

    #[test]
    fn merge_intents_round_trip_in_both_phases() {
        let mut intent = StructuralIntent {
            collection: collection(),
            source_node_id: Some(node_id(1)),
            source_revision: String::new(),
            change: StructuralChange::Merge { target: None },
            participant_id: tx_id(b"participant"),
            phase: StructuralIntentPhase::Preparing,
        };
        assert_eq!(round_trip(&intent), intent);

        intent.source_revision = "v3".to_string();
        intent.change = StructuralChange::Merge {
            target: Some(MergeTarget {
                node_id: node_id(2),
                boundary: b"m".to_vec(),
                generation: 4,
            }),
        };
        intent.phase = StructuralIntentPhase::Ready;
        assert_eq!(round_trip(&intent), intent);
    }

    #[test]
    fn decode_rejects_inconsistent_merge_intents() {
        let valid = pb::StructuralIntent {
            collection_id: vec![0; 16],
            source_node_id: vec![1; 16],
            participant_id: vec![3; 16],
            phase: pb::structural_intent::Phase::Ready.into(),
            merge: Some(pb::MergeIntent {
                target_node_id: vec![2; 16],
                boundary: b"m".to_vec(),
                target_generation: 4,
            }),
            ..pb::StructuralIntent::default()
        };
        assert!(decode(&valid).is_ok());

        type IntentEdit = fn(&mut pb::StructuralIntent);
        let cases: [(&str, IntentEdit); 7] = [
            ("root source", |raw| {
                raw.source_node_id.clear();
                raw.is_root = true;
            }),
            ("created node", |raw| {
                raw.created_node_ids = vec![vec![3; 16]];
            }),
            ("Ready without target", |raw| {
                raw.merge = Some(pb::MergeIntent::default());
            }),
            ("Ready without boundary", |raw| {
                raw.merge.as_mut().unwrap().boundary.clear();
            }),
            ("Ready with a malformed target", |raw| {
                raw.merge.as_mut().unwrap().target_node_id = vec![2; 15];
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
            assert!(decode(&raw).is_err(), "{name}");
        }
    }

    #[test]
    fn decode_rejects_ids_that_are_not_16_bytes() {
        let valid = pb::StructuralIntent {
            collection_id: vec![0; 16],
            source_node_id: vec![1; 16],
            created_node_ids: vec![vec![2; 16]],
            split_key: b"m".to_vec(),
            participant_id: vec![3; 16],
            ..pb::StructuralIntent::default()
        };
        assert!(decode(&valid).is_ok());

        // IDs of the older string format have 22 bytes.
        for id in [vec![1; 15], vec![1; 17], b"0000000000000000000000".to_vec()] {
            let mut collection = valid.clone();
            collection.collection_id = id.clone();
            let mut source = valid.clone();
            source.source_node_id = id.clone();
            let mut created = valid.clone();
            created.created_node_ids = vec![id.clone()];
            let mut participant = valid.clone();
            participant.participant_id = id;
            for raw in [collection, source, created, participant] {
                assert!(decode(&raw).is_err(), "{raw:?}");
            }
        }

        // An empty source ID names the tree root, but no other ID can be empty.
        let mut collection = valid.clone();
        collection.collection_id.clear();
        let mut created = valid.clone();
        created.created_node_ids = vec![Vec::new()];
        let mut participant = valid;
        participant.participant_id.clear();
        for raw in [collection, created, participant] {
            assert!(decode(&raw).is_err(), "{raw:?}");
        }
    }

    #[test]
    fn golden_split_intent_encoding() {
        let intent = StructuralIntent {
            collection: collection(),
            source_node_id: Some(NodeId::from_bytes([1; 16])),
            source_revision: "v7".to_string(),
            change: StructuralChange::Split {
                created_node_ids: vec![NodeId::from_bytes([2; 16])],
                split_key: b"m".to_vec(),
            },
            participant_id: TxId::from_bytes([3; 16]),
            phase: StructuralIntentPhase::Ready,
        };
        let bytes = [
            b"\x0a\x10".as_slice(),
            &[0; 16],
            b"\x12\x10",
            &[1; 16],
            b"\x1a\x02v7\x22\x10",
            &[2; 16],
            b"\x2a\x01m\x3a\x10",
            &[3; 16],
            b"\x40\x01",
        ]
        .concat();

        assert_eq!(intent.encode(), bytes);
        assert_eq!(
            StructuralIntent::decode(&db_prefix(), &bytes).unwrap(),
            intent
        );
    }

    #[test]
    fn golden_merge_intent_encoding() {
        let intent = StructuralIntent {
            collection: collection(),
            source_node_id: Some(NodeId::from_bytes([1; 16])),
            source_revision: "v1".to_string(),
            change: StructuralChange::Merge {
                target: Some(MergeTarget {
                    node_id: NodeId::from_bytes([2; 16]),
                    boundary: b"m".to_vec(),
                    generation: 5,
                }),
            },
            participant_id: TxId::from_bytes([3; 16]),
            phase: StructuralIntentPhase::Ready,
        };
        let bytes = [
            b"\x0a\x10".as_slice(),
            &[0; 16],
            b"\x12\x10",
            &[1; 16],
            b"\x1a\x02v1\x3a\x10",
            &[3; 16],
            b"\x40\x01\x4a\x17\x0a\x10",
            &[2; 16],
            b"\x12\x01m\x18\x05",
        ]
        .concat();

        assert_eq!(intent.encode(), bytes);
        assert_eq!(
            StructuralIntent::decode(&db_prefix(), &bytes).unwrap(),
            intent
        );
    }
}
