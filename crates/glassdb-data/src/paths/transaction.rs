use std::fmt;

use crate::base64;
use crate::txid::TxId;

use super::{DbRoot, ObjectPath, PathError, TRANSACTION_SHARD_COUNT};

const TRANSACTION_MARKER: &str = "_t";

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    let (parent, encoded) = path.rsplit_once('/')?;
    let (typed, shard) = parent.rsplit_once('/')?;
    let (prefix, marker) = typed.rsplit_once('/')?;
    if marker != TRANSACTION_MARKER {
        return None;
    }
    if prefix.is_empty() {
        return Some(Err(PathError::Parse(path.to_string())));
    }
    Some(decode_parts(path, shard, encoded).and_then(|id| {
        Ok(ObjectPath::Transaction {
            db_root: DbRoot::try_from(prefix)?,
            id,
        })
    }))
}

pub(super) fn parse_suffix(suffix: &str) -> Result<TxId, PathError> {
    let expected = format!("{TRANSACTION_MARKER}/");
    let Some(rest) = suffix.strip_prefix(&expected) else {
        return Err(PathError::WrongPrefix {
            suffix: suffix.to_string(),
            expected: TRANSACTION_MARKER.to_string(),
        });
    };
    let Some((shard, encoded)) = rest.split_once('/') else {
        return Err(PathError::Parse(suffix.to_string()));
    };
    if encoded.contains('/') {
        return Err(PathError::Parse(suffix.to_string()));
    }
    decode_parts(suffix, shard, encoded)
}

pub(super) fn write_object(f: &mut fmt::Formatter<'_>, prefix: &str, id: &TxId) -> fmt::Result {
    let encoded = base64::encode(id.as_bytes());
    let shard = shard_for_encoding(&encoded);
    write!(f, "{prefix}/{TRANSACTION_MARKER}/{shard}/{encoded}")
}

pub(super) fn object(prefix: &str, id: &TxId) -> String {
    let encoded = base64::encode(id.as_bytes());
    let shard = shard_for_encoding(&encoded);
    format!("{prefix}/{TRANSACTION_MARKER}/{shard}/{encoded}")
}

pub(super) fn prefix(prefix: &str) -> String {
    format!("{prefix}/{TRANSACTION_MARKER}/")
}

pub(super) fn shard(id: &TxId) -> usize {
    let encoded = base64::encode(id.as_bytes());
    base64::decode_u12(shard_for_encoding(&encoded))
        .expect("transaction shard uses the base64 alphabet")
}

pub(super) fn shard_prefix(prefix: &str, shard: usize) -> String {
    assert!(
        shard < TRANSACTION_SHARD_COUNT,
        "transaction shard out of range"
    );
    let symbols = base64::encode_u12(shard);
    let symbols = std::str::from_utf8(&symbols).expect("base64 alphabet is ASCII");
    format!("{prefix}/{TRANSACTION_MARKER}/{symbols}/")
}

pub(super) fn encoded_id(id: &TxId) -> String {
    base64::encode(id.as_bytes())
}

fn decode_parts(source: &str, shard: &str, encoded: &str) -> Result<TxId, PathError> {
    if shard.len() != 2 || encoded.is_empty() || shard != shard_for_encoding(encoded) {
        return Err(PathError::Parse(source.to_string()));
    }
    let bytes = base64::decode(encoded)?;
    if base64::encode(&bytes) != encoded {
        return Err(PathError::Parse(source.to_string()));
    }
    Ok(TxId::from_bytes(bytes))
}

fn shard_for_encoding(encoded: &str) -> &str {
    encoded.get(..2).unwrap_or("00")
}
