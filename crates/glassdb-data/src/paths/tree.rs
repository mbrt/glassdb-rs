use std::fmt;

use crate::base64;
use crate::collection_id::CollectionId;

use super::{CollectionAddress, DbRoot, NodeToken, ObjectPath, PathError};

const COLLECTION_RECORD_MARKER: &str = "_i";
const NODE_MARKER: &str = "_n";
const TREE_ROOT_MARKER: &str = "_r";

pub(super) fn collection_prefix(db_root: &str, id: CollectionId) -> String {
    format!("{db_root}/_c/{}", base64::encode(id.as_bytes()))
}

pub(super) fn parse_collection_prefix(prefix: &str) -> Result<(&str, CollectionId), PathError> {
    let Some((db_root, encoded)) = prefix.split_once("/_c/") else {
        return Err(PathError::Parse(prefix.to_string()));
    };
    if db_root.is_empty() || encoded.is_empty() || encoded.contains('/') {
        return Err(PathError::Parse(prefix.to_string()));
    }
    let bytes = base64::decode(encoded)?;
    if base64::encode(&bytes) != encoded {
        return Err(PathError::Parse(prefix.to_string()));
    }
    let id =
        CollectionId::from_slice(&bytes).ok_or_else(|| PathError::Parse(prefix.to_string()))?;
    Ok((db_root, id))
}

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    if let Some((prefix, token)) = path.rsplit_once("/_n/") {
        return Some(
            if prefix.is_empty() || token.is_empty() || token.contains('/') {
                Err(PathError::Parse(path.to_string()))
            } else {
                parse_collection(prefix).and_then(|collection| {
                    Ok(ObjectPath::Node {
                        collection,
                        token: NodeToken::try_from(token)?,
                    })
                })
            },
        );
    }
    if let Some(prefix) = path.strip_suffix("/_i") {
        return Some(if prefix.is_empty() {
            Err(PathError::Parse(path.to_string()))
        } else {
            parse_collection(prefix).map(|collection| ObjectPath::CollectionRecord { collection })
        });
    }
    if let Some(prefix) = path.strip_suffix("/_r") {
        return Some(if prefix.is_empty() {
            Err(PathError::Parse(path.to_string()))
        } else {
            parse_collection(prefix).map(|collection| ObjectPath::TreeRoot { collection })
        });
    }
    None
}

pub(super) fn write_collection_record(f: &mut fmt::Formatter<'_>, prefix: &str) -> fmt::Result {
    write!(f, "{prefix}/{COLLECTION_RECORD_MARKER}")
}

pub(super) fn write_tree_root(f: &mut fmt::Formatter<'_>, prefix: &str) -> fmt::Result {
    write!(f, "{prefix}/{TREE_ROOT_MARKER}")
}

pub(super) fn write_node(f: &mut fmt::Formatter<'_>, prefix: &str, token: &str) -> fmt::Result {
    write!(f, "{prefix}/{NODE_MARKER}/{token}")
}

pub(super) fn nodes_prefix(prefix: &str) -> String {
    format!("{prefix}/{NODE_MARKER}/")
}

fn parse_collection(prefix: &str) -> Result<CollectionAddress, PathError> {
    let (db_root, id) = parse_collection_prefix(prefix)?;
    DbRoot::try_from(db_root)?;
    Ok(CollectionAddress::new(db_root, id))
}
