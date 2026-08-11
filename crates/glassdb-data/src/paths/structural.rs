use std::fmt;

use crate::base64;
use crate::txid::TxId;

use super::{DbRoot, ObjectPath, PathError, StructuralRecordId};

const STRUCTURAL_MARKER: &str = "_s";

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    let (db_root, suffix) = path.split_once("/_s/")?;
    Some(
        parse_parts(path, db_root, suffix).and_then(|(participant, record_id)| {
            Ok(ObjectPath::StructuralRecord {
                db_root: DbRoot::try_from(db_root)?,
                participant,
                record_id: StructuralRecordId::try_from(record_id)?,
            })
        }),
    )
}

pub(super) fn parse_legacy(path: &str) -> Result<(TxId, String), PathError> {
    let Some((db_root, suffix)) = path.split_once("/_s/") else {
        return Err(PathError::Parse(path.to_string()));
    };
    parse_parts(path, db_root, suffix)
}

pub(super) fn write_record(
    f: &mut fmt::Formatter<'_>,
    db_root: &str,
    participant: &TxId,
    record_id: &str,
) -> fmt::Result {
    write!(
        f,
        "{db_root}/{STRUCTURAL_MARKER}/{}/{record_id}",
        base64::encode(participant.as_bytes())
    )
}

pub(super) fn record(db_root: &str, participant: &TxId, record_id: &str) -> String {
    format!(
        "{db_root}/{STRUCTURAL_MARKER}/{}/{record_id}",
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
    let Some((encoded_participant, record_id)) = suffix.split_once('/') else {
        return Err(PathError::Parse(source.to_string()));
    };
    if db_root.is_empty()
        || encoded_participant.is_empty()
        || record_id.is_empty()
        || record_id.contains('/')
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
    Ok((participant, record_id.to_string()))
}
