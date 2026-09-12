use std::fmt;

use crate::base64;
use crate::txid::TxId;

use super::{DbRoot, ObjectPath, PathError};

const TRANSACTION_MARKER: &str = "_t";

pub(super) fn parse_object(path: &str) -> Option<Result<ObjectPath, PathError>> {
    let (parent, encoded) = path.rsplit_once('/')?;
    let (parent, b) = parent.rsplit_once('/')?;
    let (typed, a) = parent.rsplit_once('/')?;
    let (prefix, marker) = typed.rsplit_once('/')?;
    if marker != TRANSACTION_MARKER {
        return None;
    }
    if prefix.is_empty() {
        return Some(Err(PathError::Parse(path.to_string())));
    }
    Some(decode_parts(path, a, b, encoded).and_then(|id| {
        Ok(ObjectPath::Transaction {
            db_root: DbRoot::try_from(prefix)?,
            id,
        })
    }))
}

pub(super) fn write_object(f: &mut fmt::Formatter<'_>, prefix: &str, id: &TxId) -> fmt::Result {
    let encoded = base64::encode(id.as_bytes());
    let shard = shard_for_encoding(&encoded);
    write!(
        f,
        "{prefix}/{TRANSACTION_MARKER}/{}/{}/{encoded}",
        &shard[..1],
        &shard[1..]
    )
}

pub(super) fn shard(id: &TxId) -> usize {
    let encoded = base64::encode(id.as_bytes());
    base64::decode_u12(shard_for_encoding(&encoded))
        .expect("transaction shard uses the base64 alphabet")
}

pub(super) fn shard_prefix(prefix: &str, shard: usize) -> String {
    let symbols = base64::encode_u12(shard);
    let symbols = std::str::from_utf8(&symbols).expect("base64 alphabet is ASCII");
    format!(
        "{prefix}/{TRANSACTION_MARKER}/{}/{}/",
        &symbols[..1],
        &symbols[1..]
    )
}

pub(super) fn scan_prefix(prefix: &str, depth: u8, index: usize) -> Result<String, PathError> {
    if depth > 2 || index >= 64usize.pow(u32::from(depth)) {
        return Err(PathError::Parse(format!(
            "invalid transaction scan prefix: {depth}/{index}"
        )));
    }
    match depth {
        0 => Ok(format!("{prefix}/{TRANSACTION_MARKER}/")),
        1 => {
            let symbols = base64::encode_u12(index * 64);
            Ok(format!(
                "{prefix}/{TRANSACTION_MARKER}/{}/",
                char::from(symbols[0])
            ))
        }
        _ => Ok(shard_prefix(prefix, index)),
    }
}

fn decode_parts(source: &str, a: &str, b: &str, encoded: &str) -> Result<TxId, PathError> {
    let shard = shard_for_encoding(encoded).as_bytes();
    if a.len() != 1
        || b.len() != 1
        || encoded.is_empty()
        || a.as_bytes() != &shard[..1]
        || b.as_bytes() != &shard[1..]
    {
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
