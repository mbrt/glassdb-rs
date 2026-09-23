use std::fmt;

use crate::base64;
use crate::collection_id::CollectionId;

use super::{CollectionAddress, DbPrefix, NodeToken, ObjectPath, PathError};

const COLLECTION_RECORD_MARKER: &str = "_i";
const NODE_MARKER: &str = "_n";
const TREE_ROOT_MARKER: &str = "_r";

pub(super) fn collection_prefix(db_prefix: &str, id: CollectionId) -> String {
    format!("{db_prefix}/_c/{}", base64::encode(id.as_bytes()))
}

pub(super) fn parse_collection_prefix(prefix: &str) -> Result<(&str, CollectionId), PathError> {
    let Some((db_prefix, encoded)) = prefix.split_once("/_c/") else {
        return Err(PathError::Parse(prefix.to_string()));
    };
    if db_prefix.is_empty() || encoded.is_empty() || encoded.contains('/') {
        return Err(PathError::Parse(prefix.to_string()));
    }
    let bytes = base64::decode(encoded)?;
    if base64::encode(&bytes) != encoded {
        return Err(PathError::Parse(prefix.to_string()));
    }
    let id =
        CollectionId::from_slice(&bytes).ok_or_else(|| PathError::Parse(prefix.to_string()))?;
    Ok((db_prefix, id))
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

pub(super) fn write_collection_record(
    f: &mut fmt::Formatter<'_>,
    collection: &CollectionAddress,
) -> fmt::Result {
    write_collection_prefix(f, collection)?;
    write!(f, "/{COLLECTION_RECORD_MARKER}")
}

pub(super) fn write_tree_root(
    f: &mut fmt::Formatter<'_>,
    collection: &CollectionAddress,
) -> fmt::Result {
    write_collection_prefix(f, collection)?;
    write!(f, "/{TREE_ROOT_MARKER}")
}

pub(super) fn write_node(
    f: &mut fmt::Formatter<'_>,
    collection: &CollectionAddress,
    token: &str,
) -> fmt::Result {
    write_collection_prefix(f, collection)?;
    write!(f, "/{NODE_MARKER}/{token}")
}

pub(super) fn nodes_prefix(prefix: &str) -> String {
    format!("{prefix}/{NODE_MARKER}/")
}

fn parse_collection(prefix: &str) -> Result<CollectionAddress, PathError> {
    let (db_prefix, id) = parse_collection_prefix(prefix)?;
    DbPrefix::try_from(db_prefix)?;
    Ok(CollectionAddress::new(db_prefix, id))
}

fn write_collection_prefix(
    f: &mut fmt::Formatter<'_>,
    collection: &CollectionAddress,
) -> fmt::Result {
    write!(
        f,
        "{}/_c/{}",
        collection.db_prefix(),
        base64::encode(collection.id().as_bytes())
    )
}
