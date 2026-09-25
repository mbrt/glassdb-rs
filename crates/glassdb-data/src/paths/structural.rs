use std::fmt;

use crate::base64;
use crate::ids::{StructuralIntentId, TxId};

use super::{DbPrefix, ObjectPath, PathError, parse_id};

const STRUCTURAL_MARKER: &str = "_s";

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    let (db_prefix, suffix) = path.split_once("/_s/")?;
    Some(
        parse_parts(path, db_prefix, suffix).and_then(|(participant, intent_id)| {
            Ok(ObjectPath::StructuralIntent {
                db_prefix: DbPrefix::try_from(db_prefix)?,
                participant,
                intent_id: parse_id("structural intent ID", intent_id)?,
            })
        }),
    )
}

pub(super) fn write_intent(
    f: &mut fmt::Formatter<'_>,
    db_prefix: &str,
    participant: &TxId,
    intent_id: &StructuralIntentId,
) -> fmt::Result {
    write!(
        f,
        "{db_prefix}/{STRUCTURAL_MARKER}/{}/{}",
        base64::encode(participant.as_bytes()),
        base64::encode(intent_id.as_bytes())
    )
}

pub(super) fn directory(db_prefix: &str) -> String {
    format!("{db_prefix}/{STRUCTURAL_MARKER}/")
}

pub(super) fn participant_directory(db_prefix: &str, participant: &TxId) -> String {
    format!(
        "{db_prefix}/{STRUCTURAL_MARKER}/{}/",
        base64::encode(participant.as_bytes())
    )
}

fn parse_parts<'a>(
    source: &str,
    db_prefix: &str,
    suffix: &'a str,
) -> Result<(TxId, &'a str), PathError> {
    let Some((encoded_participant, intent_id)) = suffix.split_once('/') else {
        return Err(PathError::Parse(source.to_string()));
    };
    if db_prefix.is_empty()
        || encoded_participant.is_empty()
        || intent_id.is_empty()
        || intent_id.contains('/')
    {
        return Err(PathError::Parse(source.to_string()));
    }
    let participant = parse_id("transaction ID", encoded_participant)?;
    Ok((participant, intent_id))
}
