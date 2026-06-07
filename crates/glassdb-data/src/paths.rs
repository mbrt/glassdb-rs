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
}

impl Type {
    /// Returns the path type marker string (`_k`, `_c`, `_t`, `_i`, or `""`).
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Unknown => "",
            Type::Key => "_k",
            Type::Collection => "_c",
            Type::Transaction => "_t",
            Type::CollectionInfo => "_i",
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
    // Build the `prefix/type/base64(a)` path in a single allocation: encode the
    // payload straight into the output buffer instead of through an intermediate
    // base64 string. This runs on every key/collection/transaction path.
    let cat = category.as_str();
    let mut s = String::with_capacity(prefix.len() + cat.len() + 2 + a.len().div_ceil(3) * 4);
    s.push_str(prefix);
    s.push('/');
    s.push_str(cat);
    s.push('/');
    base64::encode_into(a, &mut s);
    s
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
