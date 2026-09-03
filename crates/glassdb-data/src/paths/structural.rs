use std::fmt;

use crate::base64;
use crate::txid::TxId;

use super::{DbRoot, ObjectPath, PathError, StructuralIntentId};

const STRUCTURAL_MARKER: &str = "_s";

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    let (db_root, suffix) = path.split_once("/_s/")?;
    Some(
        parse_parts(path, db_root, suffix).and_then(|(participant, intent_id)| {
            Ok(ObjectPath::StructuralIntent {
                db_root: DbRoot::try_from(db_root)?,
                participant,
                intent_id: StructuralIntentId::try_from(intent_id)?,
            })
        }),
    )
}

pub(super) fn write_intent(
    f: &mut fmt::Formatter<'_>,
    db_root: &str,
    participant: &TxId,
    intent_id: &str,
) -> fmt::Result {
    write!(
        f,
        "{db_root}/{STRUCTURAL_MARKER}/{}/{intent_id}",
        base64::encode(participant.as_bytes())
    )
}

pub(super) fn directory(db_root: &str) -> String {
    format!("{db_root}/{STRUCTURAL_MARKER}/")
}

pub(super) fn participant_directory(db_root: &str, participant: &TxId) -> String {
    format!(
        "{db_root}/{STRUCTURAL_MARKER}/{}/",
        base64::encode(participant.as_bytes())
    )
}

fn parse_parts(source: &str, db_root: &str, suffix: &str) -> Result<(TxId, String), PathError> {
    let Some((encoded_participant, intent_id)) = suffix.split_once('/') else {
        return Err(PathError::Parse(source.to_string()));
    };
    if db_root.is_empty()
        || encoded_participant.is_empty()
        || intent_id.is_empty()
        || intent_id.contains('/')
    {
        return Err(PathError::Parse(source.to_string()));
    }
    let bytes = base64::decode(encoded_participant)?;
    if base64::encode(&bytes) != encoded_participant {
        return Err(PathError::Parse(source.to_string()));
    }
    let participant = TxId::from_bytes(bytes);
    if participant.is_unset() {
        return Err(PathError::Parse(source.to_string()));
    }
    Ok((participant, intent_id.to_string()))
}
