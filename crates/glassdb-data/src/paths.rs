//! Storage path encoding/decoding. Ported from the Go `internal/data/paths`
//! package. Paths have the form `{prefix}/{type}/{base64(payload)}`, except
//! collection-info objects which are `{prefix}/_i`.

use crate::base64;
use crate::txid::TxId;

/// The category of a storage path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Unknown,
    Key,
    Collection,
    Transaction,
    CollectionInfo,
    Shard,
}

impl Type {
    /// Returns the path type marker string (`_k`, `_c`, `_t`, `_i`, `_s`, or
    /// `""`).
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Unknown => "",
            Type::Key => "_k",
            Type::Collection => "_c",
            Type::Transaction => "_t",
            Type::CollectionInfo => "_i",
            Type::Shard => "_s",
        }
    }
}

/// Error returned by path parsing/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    /// The path did not have the expected `prefix/type/suffix` structure.
    Parse(String),
    /// The suffix did not start with the expected type marker.
    WrongPrefix { suffix: String, expected: String },
    /// The base64 payload could not be decoded.
    Decode(base64::DecodeError),
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::Parse(p) => write!(f, "expected path with >=3 parts, got {p:?}"),
            PathError::WrongPrefix { suffix, expected } => {
                write!(f, "got path {suffix:?}, expected prefix {expected:?}")
            }
            PathError::Decode(e) => write!(f, "decoding path: {e}"),
        }
    }
}

impl std::error::Error for PathError {}

impl From<base64::DecodeError> for PathError {
    fn from(e: base64::DecodeError) -> Self {
        PathError::Decode(e)
    }
}

/// The result of [`parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseResult {
    pub prefix: String,
    pub suffix: String,
    pub typ: Type,
}

/// Encodes a key into a storage path under `prefix`.
pub fn from_key(prefix: &str, key: &[u8]) -> String {
    prefix_encode(prefix, Type::Key, key)
}

/// Decodes a key from a storage path suffix (e.g. `_k/<b64>`).
pub fn to_key(suffix: &str) -> Result<Vec<u8>, PathError> {
    decode(Type::Key, suffix)
}

/// Splits a full key path (`{prefix}/_k/<b64>`) into its collection prefix and
/// decoded raw key bytes, the inverse of [`from_key`]. Unlike [`to_key`] (which
/// decodes a type-marked suffix), this takes a whole path.
pub fn split_key(path: &str) -> Result<(String, Vec<u8>), PathError> {
    let pr = parse(path)?;
    if pr.typ != Type::Key {
        return Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: Type::Key.as_str().to_string(),
        });
    }
    Ok((pr.prefix, base64::decode(&pr.suffix)?))
}

/// Returns the listing prefix for all keys under `prefix`.
pub fn keys_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Key)
}

/// Encodes a collection name into a storage path under `prefix`.
pub fn from_collection(prefix: &str, name: &[u8]) -> String {
    prefix_encode(prefix, Type::Collection, name)
}

/// Decodes a collection name from a storage path suffix (e.g. `_c/<b64>`).
pub fn to_collection(suffix: &str) -> Result<Vec<u8>, PathError> {
    decode(Type::Collection, suffix)
}

/// Returns the storage path for the collection-info object under `prefix`.
pub fn collection_info(prefix: &str) -> String {
    crate::gopath::join(&[prefix, "_i"])
}

/// Reports whether `p` refers to a collection-info object.
pub fn is_collection_info(p: &str) -> bool {
    p.ends_with("/_i")
}

/// Returns the listing prefix for all collections under `prefix`.
pub fn collections_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Collection)
}

/// Encodes a transaction ID into a storage path under `prefix`.
pub fn from_transaction(prefix: &str, id: &TxId) -> String {
    prefix_encode(prefix, Type::Transaction, id.as_bytes())
}

/// Decodes a transaction ID from a storage path suffix (e.g. `_t/<b64>`).
pub fn to_transaction(suffix: &str) -> Result<TxId, PathError> {
    Ok(TxId::from_bytes(decode(Type::Transaction, suffix)?))
}

/// Returns the listing prefix for all transaction objects under `prefix`.
pub fn transactions_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Transaction)
}

/// Decodes the transaction ID from a full transaction object path
/// (`{prefix}/_t/<b64>`), the inverse of [`from_transaction`]. Unlike
/// [`to_transaction`] (which decodes a type-marked suffix), this takes a whole
/// path as returned by a transaction listing.
pub fn transaction_id_of(path: &str) -> Result<TxId, PathError> {
    let pr = parse(path)?;
    if pr.typ != Type::Transaction {
        return Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: Type::Transaction.as_str().to_string(),
        });
    }
    Ok(TxId::from_bytes(base64::decode(&pr.suffix)?))
}

/// Returns the storage path for shard `index` under `prefix`.
///
/// The index is a fixed-width zero-padded decimal so shard paths are a stable,
/// lexicographically ordered function of the index (ADR-017).
pub fn from_shard(prefix: &str, index: u32) -> String {
    debug_assert!(
        index < crate::shard::SHARD_COUNT,
        "shard index {index} out of range"
    );
    format!(
        "{}/{}/{:0width$}",
        prefix,
        Type::Shard.as_str(),
        index,
        width = crate::shard::SHARD_INDEX_WIDTH
    )
}

/// Decodes a shard index from a storage path suffix (e.g. `_s/0042`).
pub fn to_shard(suffix: &str) -> Result<u32, PathError> {
    let pfx = format!("{}/", Type::Shard.as_str());
    let rest = suffix
        .strip_prefix(&pfx)
        .ok_or_else(|| PathError::WrongPrefix {
            suffix: suffix.to_string(),
            expected: Type::Shard.as_str().to_string(),
        })?;
    rest.parse()
        .map_err(|_| PathError::Parse(suffix.to_string()))
}

/// Returns the listing prefix for all shards under `prefix`.
pub fn shards_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Shard)
}

/// Decodes the shard index from a full shard object path (`{prefix}/_s/<idx>`),
/// the inverse of [`from_shard`]. Unlike [`to_shard`] (which decodes a
/// type-marked suffix), this takes a whole path as returned by a shard listing.
pub fn shard_index_of(path: &str) -> Result<u32, PathError> {
    let pr = parse(path)?;
    if pr.typ != Type::Shard {
        return Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: Type::Shard.as_str().to_string(),
        });
    }
    pr.suffix
        .parse()
        .map_err(|_| PathError::Parse(path.to_string()))
}

/// Splits a storage path into its prefix, type, and suffix components.
pub fn parse(p: &str) -> Result<ParseResult, PathError> {
    if is_collection_info(p) {
        return Ok(ParseResult {
            prefix: p[..p.len() - 3].to_string(),
            suffix: String::new(),
            typ: Type::CollectionInfo,
        });
    }
    let (prefix_idx, type_idx) =
        path_parts_indexes(p).ok_or_else(|| PathError::Parse(p.to_string()))?;
    let prefix = &p[..prefix_idx];
    let typ_str = &p[prefix_idx + 1..type_idx];
    let suffix = &p[type_idx + 1..];
    let typ = match typ_str {
        "_k" => Type::Key,
        "_c" => Type::Collection,
        "_t" => Type::Transaction,
        "_s" => Type::Shard,
        _ => Type::Unknown,
    };
    Ok(ParseResult {
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
        typ,
    })
}

fn path_parts_indexes(p: &str) -> Option<(usize, usize)> {
    let type_idx = p.rfind('/')?;
    if type_idx == 0 {
        return None;
    }
    let prefix_idx = p[..type_idx - 1].rfind('/')?;
    Some((prefix_idx, type_idx))
}

fn typed_prefix(prefix: &str, t: Type) -> String {
    format!("{}/{}/", prefix, t.as_str())
}

fn prefix_encode(prefix: &str, category: Type, a: &[u8]) -> String {
    format!("{}/{}/{}", prefix, category.as_str(), base64::encode(a))
}

fn decode(category: Type, suffix: &str) -> Result<Vec<u8>, PathError> {
    let pfx = format!("{}/", category.as_str());
    match suffix.strip_prefix(&pfx) {
        Some(rest) => Ok(base64::decode(rest)?),
        None => Err(PathError::WrongPrefix {
            suffix: suffix.to_string(),
            expected: category.as_str().to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trip() {
        let p = from_key("foo/bar", b"Hello");
        assert!(p.starts_with("foo/bar/_k/"));
        let suffix = p.strip_prefix("foo/bar/").unwrap();
        assert_eq!(to_key(suffix).unwrap(), b"Hello");
    }

    #[test]
    fn parse_key() {
        let p = from_key("foo/bar", b"Hello");
        let r = parse(&p).unwrap();
        assert_eq!(r.prefix, "foo/bar");
        assert_eq!(r.typ, Type::Key);
        // suffix is just the base64 component.
        assert_eq!(to_key(&format!("_k/{}", r.suffix)).unwrap(), b"Hello");
    }

    #[test]
    fn split_key_round_trip_and_errors() {
        let (prefix, key) = split_key(&from_key("foo/bar", b"Hello")).unwrap();
        assert_eq!(prefix, "foo/bar");
        assert_eq!(key, b"Hello");
        // A non-key path is rejected.
        assert!(matches!(
            split_key(&from_shard("db/coll", 1)),
            Err(PathError::WrongPrefix { .. })
        ));
        // A malformed path (no type segment) is a parse error.
        assert!(matches!(split_key("db"), Err(PathError::Parse(_))));
    }

    #[test]
    fn collection_info_paths() {
        assert_eq!(collection_info("foo/bar"), "foo/bar/_i");
        assert!(is_collection_info("foo/bar/_i"));
        let r = parse("foo/bar/_i").unwrap();
        assert_eq!(r.prefix, "foo/bar");
        assert_eq!(r.typ, Type::CollectionInfo);
    }

    #[test]
    fn transaction_round_trip() {
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        let p = from_transaction("db", &id);
        let r = parse(&p).unwrap();
        assert_eq!(r.typ, Type::Transaction);
        assert_eq!(to_transaction(&format!("_t/{}", r.suffix)).unwrap(), id);
    }

    #[test]
    fn keys_prefix_format() {
        assert_eq!(keys_prefix("db/coll"), "db/coll/_k/");
        assert_eq!(collections_prefix("db/coll"), "db/coll/_c/");
        assert_eq!(shards_prefix("db/coll"), "db/coll/_s/");
        assert_eq!(transactions_prefix("db"), "db/_t/");
    }

    #[test]
    fn transaction_id_of_round_trip_and_errors() {
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        assert_eq!(transaction_id_of(&from_transaction("db", &id)).unwrap(), id);
        // A non-transaction path is rejected.
        assert!(matches!(
            transaction_id_of(&from_key("db/coll", b"k")),
            Err(PathError::WrongPrefix { .. })
        ));
        // A malformed path (no type segment) is a parse error.
        assert!(matches!(transaction_id_of("db"), Err(PathError::Parse(_))));
    }

    #[test]
    fn shard_round_trip() {
        let p = from_shard("db/coll", 42);
        assert_eq!(p, "db/coll/_s/0042");
        let r = parse(&p).unwrap();
        assert_eq!(r.prefix, "db/coll");
        assert_eq!(r.typ, Type::Shard);
        assert_eq!(r.suffix, "0042");
        assert_eq!(to_shard(&format!("_s/{}", r.suffix)).unwrap(), 42);
    }

    #[test]
    fn to_shard_errors() {
        assert!(matches!(
            to_shard("_k/0042"),
            Err(PathError::WrongPrefix { .. })
        ));
        assert!(matches!(
            to_shard("_s/notanumber"),
            Err(PathError::Parse(_))
        ));
    }

    #[test]
    fn shard_index_of_round_trip_and_errors() {
        assert_eq!(shard_index_of(&from_shard("db/coll", 42)).unwrap(), 42);
        // A non-shard path is rejected.
        assert!(matches!(
            shard_index_of(&from_key("db/coll", b"k")),
            Err(PathError::WrongPrefix { .. })
        ));
        // A malformed path (no type segment) is a parse error.
        assert!(matches!(shard_index_of("db"), Err(PathError::Parse(_))));
    }

    // Golden vectors produced by the Go implementation, to guarantee
    // byte-for-byte compatibility of the path encoding.
    #[test]
    fn golden_vectors_match_go() {
        assert_eq!(base64::encode(b"Hello"), "H6KgQ6w");
        assert_eq!(base64::encode(&[0, 1, 2, 3, 4]), "00420kF");
        assert_eq!(base64::encode(b"ab"), "NL8");
        assert_eq!(from_key("foo/bar", b"Hello"), "foo/bar/_k/H6KgQ6w");
        assert_eq!(from_collection("db", b"settings"), "db/_c/RqKoS6_iOrB");
        assert_eq!(
            from_transaction("db", &TxId::from_bytes(vec![1, 2, 3, 4])),
            "db/_t/0F8310"
        );
        assert_eq!(collection_info("db/root"), "db/root/_i");

        let r = parse("foo/bar/_k/H6KgQ6w").unwrap();
        assert_eq!(r.prefix, "foo/bar");
        assert_eq!(r.suffix, "H6KgQ6w");
        assert_eq!(r.typ, Type::Key);
    }
}
