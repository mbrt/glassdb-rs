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
    /// A B-link tree node object (`_n/<token>`, ADR-031).
    Node,
}

impl Type {
    /// Returns the path type marker string (`_k`, `_c`, `_t`, `_i`, `_n`, or
    /// `""`).
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Unknown => "",
            Type::Key => "_k",
            Type::Collection => "_c",
            Type::Transaction => "_t",
            Type::CollectionInfo => "_i",
            Type::Node => "_n",
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
    format!("{prefix}/_i")
}

/// Reports whether `p` refers to a collection-info object.
pub fn is_collection_info(p: &str) -> bool {
    p.ends_with("/_i")
}

/// Returns the listing prefix for all collections under `prefix`.
pub fn collections_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Collection)
}

/// Splits a collection `prefix` into its parent collection prefix and this
/// collection's decoded name, but only when the parent is *itself* a collection
/// (and thus owns a root `_i` that holds a subcollection directory).
///
/// A top-level collection's parent is the database, which has no root, so this
/// returns `None`. It also returns `None` for any non-collection path.
pub fn parent_collection(prefix: &str) -> Option<(String, Vec<u8>)> {
    let pr = parse(prefix).ok()?;
    if pr.typ != Type::Collection {
        return None;
    }
    if parse(&pr.prefix).map(|p| p.typ) != Ok(Type::Collection) {
        return None;
    }
    let name = base64::decode(&pr.suffix).ok()?;
    Some((pr.prefix, name))
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

/// Returns the storage path for the B-link node named `token` under `prefix`
/// (`{prefix}/_n/<token>`, ADR-031).
///
/// The token is an opaque identity string (typically from [`random_node_token`]),
/// not a computed index: the tree is dynamic, so a node is addressed by
/// descending to it, never by formula.
pub fn from_node(prefix: &str, token: &str) -> String {
    format!("{}/{}/{}", prefix, Type::Node.as_str(), token)
}

/// Returns the listing prefix for all B-link node objects under `prefix`.
pub fn nodes_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Node)
}

/// The database's structural-log directory (`{db}/_s/`), where the database
/// name is the leading path segment of a collection prefix. Recovery lists it
/// to rediscover, after a restart, every in-progress split's write-ahead record
/// across all collections (ADR-032). Replaces ADR-031's `_g/` registry.
pub fn structural_log_dir(db_root: &str) -> String {
    format!("{db_root}/_s/")
}

/// The structural-log record path for `record_id` under `db_root`
/// (`{db}/_s/<record_id>`). `record_id` is a freshly random, collision-free
/// node token, so concurrent splits never share a record (ADR-032).
pub fn structural_log_record(db_root: &str, record_id: &str) -> String {
    format!("{db_root}/_s/{record_id}")
}

/// Decodes the record id from a structural-log record path, the inverse of
/// [`structural_log_record`].
pub fn structural_log_id_of(path: &str) -> Result<String, PathError> {
    match path.rsplit('/').next() {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(PathError::Parse(format!(
            "structural-log path has no record id: {path:?}"
        ))),
    }
}

/// The database name for a collection `prefix`: its leading path segment. A
/// database name is validated alphanumeric, so it never contains a `/`.
pub fn db_root_of(prefix: &str) -> &str {
    match prefix.find('/') {
        Some(i) => &prefix[..i],
        None => prefix,
    }
}

/// Decodes a node token from a full node object path (`{prefix}/_n/<token>`),
/// the inverse of [`from_node`].
pub fn node_token_of(path: &str) -> Result<String, PathError> {
    let pr = parse(path)?;
    if pr.typ != Type::Node {
        return Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: Type::Node.as_str().to_string(),
        });
    }
    Ok(pr.suffix)
}

/// Number of random bytes behind a node token. 128 bits of entropy makes
/// accidental collisions negligible and, crucially, spreads freshly created
/// nodes across object-store partitions instead of clustering them (ADR-031).
const NODE_TOKEN_BYTES: usize = 16;

/// Mints a fresh, random B-link node token.
///
/// The token is deliberately random rather than monotonic: object stores
/// partition by key prefix, so monotonically increasing names would pile new
/// nodes onto one partition and accidentally hot-key the backend (ADR-031). It
/// draws from the same seeded-under-simulation entropy as [`crate::TxId`], so
/// DST replays stay byte-identical.
pub fn random_node_token() -> String {
    let mut b = [0u8; NODE_TOKEN_BYTES];
    crate::entropy::fill_random(&mut b);
    base64::encode(&b)
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
        "_n" => Type::Node,
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
            split_key(&from_node("db/coll", "tok")),
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
        assert_eq!(transactions_prefix("db"), "db/_t/");
    }

    #[test]
    fn parent_collection_identifies_subcollection_owner() {
        // A top-level collection's parent is the database, which owns no root.
        assert_eq!(parent_collection(&from_collection("db", b"top")), None);

        // A subcollection's parent is the collection that owns its directory.
        let parent = from_collection("db", b"parent");
        let child = from_collection(&parent, b"child");
        assert_eq!(
            parent_collection(&child),
            Some((parent.clone(), b"child".to_vec()))
        );

        // Nesting composes: the owner is always the direct parent.
        let grandchild = from_collection(&child, b"grandchild");
        assert_eq!(
            parent_collection(&grandchild),
            Some((child, b"grandchild".to_vec()))
        );

        // Non-collection paths have no collection parent.
        assert_eq!(parent_collection(&from_key("db/coll", b"k")), None);
        assert_eq!(parent_collection("db"), None);
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
    fn node_round_trip_and_errors() {
        let p = from_node("db/coll", "AbC123");
        assert_eq!(p, "db/coll/_n/AbC123");
        let r = parse(&p).unwrap();
        assert_eq!(r.prefix, "db/coll");
        assert_eq!(r.typ, Type::Node);
        assert_eq!(r.suffix, "AbC123");
        assert_eq!(node_token_of(&p).unwrap(), "AbC123");
        assert_eq!(nodes_prefix("db/coll"), "db/coll/_n/");
        // A non-node path is rejected.
        assert!(matches!(
            node_token_of(&from_key("db/coll", b"k")),
            Err(PathError::WrongPrefix { .. })
        ));
        // A malformed path (no type segment) is a parse error.
        assert!(matches!(node_token_of("db"), Err(PathError::Parse(_))));
    }

    #[test]
    fn structural_log_record_round_trip() {
        // A structural-log record lives under the db's `_s/` dir, keyed by the
        // split's freshly random record id; the record id decodes back out.
        let record_id = "RqKoS6_iOrB";
        let path = structural_log_record("db", record_id);
        assert_eq!(path, "db/_s/RqKoS6_iOrB");
        assert!(path.starts_with(&structural_log_dir("db")));
        assert_eq!(structural_log_id_of(&path).unwrap(), record_id);
        assert_eq!(structural_log_dir("db"), "db/_s/");

        // The db root of a (nested) collection prefix is its leading segment.
        assert_eq!(db_root_of("db/_c/AAA/_c/BBB"), "db");
        assert_eq!(db_root_of("db"), "db");
    }

    #[test]
    fn random_node_token_is_a_valid_decodable_token() {
        let t = random_node_token();
        // The token round-trips through a node path.
        assert_eq!(node_token_of(&from_node("db/coll", &t)).unwrap(), t);
        // It is our order-preserving base64 of 16 random bytes.
        assert_eq!(base64::decode(&t).unwrap().len(), 16);
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
