//! Collection/key references and physical backend-object paths.

use std::sync::Arc;

use crate::base64;
use crate::collection_id::CollectionId;
use crate::txid::TxId;

/// The physical address of one collection incarnation within a database.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionAddress {
    db_root: Arc<str>,
    id: CollectionId,
}

impl CollectionAddress {
    /// Creates an address from a database root and collection identity.
    pub fn new(db_root: impl Into<Arc<str>>, id: CollectionId) -> Self {
        let db_root = db_root.into();
        assert!(!db_root.is_empty(), "database root must not be empty");
        CollectionAddress { db_root, id }
    }

    /// Creates the permanent root collection address for `db_root`.
    pub fn root(db_root: impl Into<Arc<str>>) -> Self {
        Self::new(db_root, CollectionId::root())
    }

    /// Returns this address's database root.
    pub fn db_root(&self) -> &str {
        &self.db_root
    }

    /// Returns this collection's stable incarnation identity.
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Renders the collection prefix used for physical backend objects.
    pub fn physical_prefix(&self) -> String {
        format!("{}/_c/{}", self.db_root, base64::encode(self.id.as_bytes()))
    }

    /// Parses an incarnation-addressed physical collection prefix.
    pub fn from_physical_prefix(prefix: &str) -> Result<Self, PathError> {
        let Some((db_root, encoded)) = prefix.split_once("/_c/") else {
            return Err(PathError::Parse(prefix.to_string()));
        };
        if db_root.is_empty() || encoded.is_empty() || encoded.contains('/') {
            return Err(PathError::Parse(prefix.to_string()));
        }
        let bytes = base64::decode(encoded)?;
        let id =
            CollectionId::from_slice(&bytes).ok_or_else(|| PathError::Parse(prefix.to_string()))?;
        Ok(CollectionAddress::new(db_root, id))
    }
}

/// A logical key stored inside a collection leaf.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRef {
    collection: CollectionAddress,
    key: Arc<[u8]>,
}

impl KeyRef {
    /// Creates a logical key reference.
    pub fn new(collection: CollectionAddress, key: impl AsRef<[u8]>) -> Self {
        KeyRef {
            collection,
            key: Arc::from(key.as_ref()),
        }
    }

    /// Returns the containing collection.
    pub fn collection(&self) -> &CollectionAddress {
        &self.collection
    }

    /// Returns the raw key bytes used by the leaf entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

/// A physical leaf within a collection's coordination tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LeafRef {
    Root(CollectionAddress),
    Node {
        collection: CollectionAddress,
        token: Arc<str>,
    },
}

impl LeafRef {
    /// Creates a collection-root leaf reference.
    pub fn root(collection: CollectionAddress) -> Self {
        LeafRef::Root(collection)
    }

    /// Creates a standalone-node leaf reference.
    pub fn node(collection: CollectionAddress, token: impl Into<Arc<str>>) -> Self {
        LeafRef::Node {
            collection,
            token: token.into(),
        }
    }

    /// Returns the collection whose tree contains this leaf.
    pub fn collection(&self) -> &CollectionAddress {
        match self {
            LeafRef::Root(collection) | LeafRef::Node { collection, .. } => collection,
        }
    }

    /// Returns the standalone node token, or `None` for the collection root.
    pub fn node_token(&self) -> Option<&str> {
        match self {
            LeafRef::Root(_) => None,
            LeafRef::Node { token, .. } => Some(token),
        }
    }

    /// Renders the exact physical backend object path of this leaf.
    pub fn physical_path(&self) -> String {
        match self {
            LeafRef::Root(collection) => collection_info(&collection.physical_prefix()),
            LeafRef::Node { collection, token } => from_node(&collection.physical_prefix(), token),
        }
    }

    /// Parses a physical collection-root or node path.
    pub fn from_physical_path(path: &str) -> Result<Self, PathError> {
        if let Some(prefix) = path.strip_suffix("/_i") {
            return Ok(LeafRef::root(CollectionAddress::from_physical_prefix(
                prefix,
            )?));
        }
        let Some((prefix, token)) = path.rsplit_once("/_n/") else {
            return Err(PathError::Parse(path.to_string()));
        };
        if token.is_empty() || token.contains('/') {
            return Err(PathError::Parse(path.to_string()));
        }
        Ok(LeafRef::node(
            CollectionAddress::from_physical_prefix(prefix)?,
            token,
        ))
    }
}

/// Number of deterministic transaction-log shards (two base64 symbols).
pub const TRANSACTION_SHARD_COUNT: usize = 64 * 64;

/// The category of a storage path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Unknown,
    Transaction,
    CollectionInfo,
    /// A B-link tree node object (`_n/<token>`, ADR-031).
    Node,
}

impl Type {
    /// Returns the physical object marker (`_t`, `_i`, `_n`, or `""`).
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Unknown => "",
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

/// Returns the storage path for the collection-info object under `prefix`.
pub fn collection_info(prefix: &str) -> String {
    format!("{prefix}/_i")
}

/// Reports whether `p` refers to a collection-info object.
pub fn is_collection_info(p: &str) -> bool {
    p.ends_with("/_i")
}

/// Encodes a transaction ID into a storage path under `prefix`.
pub fn from_transaction(prefix: &str, id: &TxId) -> String {
    let encoded = base64::encode(id.as_bytes());
    let shard = transaction_shard_for_encoding(&encoded);
    format!("{prefix}/{}/{shard}/{encoded}", Type::Transaction.as_str())
}

/// Decodes a transaction ID from a sharded storage path suffix
/// (`_t/<shard>/<b64>`).
pub fn to_transaction(suffix: &str) -> Result<TxId, PathError> {
    let expected = format!("{}/", Type::Transaction.as_str());
    let Some(rest) = suffix.strip_prefix(&expected) else {
        return Err(PathError::WrongPrefix {
            suffix: suffix.to_string(),
            expected: Type::Transaction.as_str().to_string(),
        });
    };
    let Some((shard, encoded)) = rest.split_once('/') else {
        return Err(PathError::Parse(suffix.to_string()));
    };
    if shard.len() != 2
        || encoded.is_empty()
        || encoded.contains('/')
        || shard != transaction_shard_for_encoding(encoded)
    {
        return Err(PathError::Parse(suffix.to_string()));
    }
    Ok(TxId::from_bytes(base64::decode(encoded)?))
}

/// Returns the listing prefix for all transaction objects under `prefix`.
pub fn transactions_prefix(prefix: &str) -> String {
    typed_prefix(prefix, Type::Transaction)
}

/// Returns the deterministic shard index for `id`.
pub fn transaction_shard(id: &TxId) -> usize {
    let encoded = base64::encode(id.as_bytes());
    base64::decode_u12(transaction_shard_for_encoding(&encoded))
        .expect("transaction shard uses the base64 alphabet")
}

/// Returns the listing prefix for one deterministic transaction-log shard.
pub fn transaction_shard_prefix(prefix: &str, shard: usize) -> String {
    assert!(
        shard < TRANSACTION_SHARD_COUNT,
        "transaction shard out of range"
    );
    let symbols = base64::encode_u12(shard);
    let symbols = std::str::from_utf8(&symbols).expect("base64 alphabet is ASCII");
    format!("{prefix}/{}/{symbols}/", Type::Transaction.as_str())
}

/// Decodes the transaction ID from a full transaction object path
/// (`{prefix}/_t/<shard>/<b64>`), the inverse of [`from_transaction`]. Unlike
/// [`to_transaction`] (which decodes a type-marked suffix), this takes a whole
/// path as returned by a transaction listing.
pub fn transaction_id_of(path: &str) -> Result<TxId, PathError> {
    let Some((_prefix, shard, encoded)) = sharded_transaction_parts(path) else {
        if path_parts_indexes(path).is_none() {
            return Err(PathError::Parse(path.to_string()));
        }
        return Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: format!("{}/<shard>/<txid>", Type::Transaction.as_str()),
        });
    };
    if shard != transaction_shard_for_encoding(encoded) {
        return Err(PathError::Parse(format!(
            "transaction shard does not match id: {path:?}"
        )));
    }
    Ok(TxId::from_bytes(base64::decode(encoded)?))
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

/// Returns the database-wide structural-log directory (`{db}/_s/`).
pub fn structural_log_dir(db_root: &str) -> String {
    format!("{db_root}/_s/")
}

/// Returns the path of one structural-log record (`{db}/_s/<record_id>`).
pub fn structural_log_record(db_root: &str, record_id: &str) -> String {
    format!("{db_root}/_s/{record_id}")
}

/// Decodes the record id from a structural-log record path.
pub fn structural_log_id_of(path: &str) -> Result<String, PathError> {
    match path.rsplit('/').next() {
        Some(id) if !id.is_empty() => Ok(id.to_string()),
        _ => Err(PathError::Parse(format!(
            "structural-log path has no record id: {path:?}"
        ))),
    }
}

/// Returns the database name at the start of a collection prefix.
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
    if let Some((prefix, shard, suffix)) = sharded_transaction_parts(p)
        && shard == transaction_shard_for_encoding(suffix)
    {
        return Ok(ParseResult {
            prefix: prefix.to_string(),
            suffix: suffix.to_string(),
            typ: Type::Transaction,
        });
    }
    let (prefix_idx, type_idx) =
        path_parts_indexes(p).ok_or_else(|| PathError::Parse(p.to_string()))?;
    let prefix = &p[..prefix_idx];
    let typ_str = &p[prefix_idx + 1..type_idx];
    let suffix = &p[type_idx + 1..];
    let typ = match typ_str {
        "_n" => Type::Node,
        _ => Type::Unknown,
    };
    Ok(ParseResult {
        prefix: prefix.to_string(),
        suffix: suffix.to_string(),
        typ,
    })
}

fn transaction_shard_for_encoding(encoded: &str) -> &str {
    encoded.get(..2).unwrap_or("00")
}

fn sharded_transaction_parts(path: &str) -> Option<(&str, &str, &str)> {
    let (parent, encoded) = path.rsplit_once('/')?;
    let (typed, shard) = parent.rsplit_once('/')?;
    let (prefix, marker) = typed.rsplit_once('/')?;
    (marker == Type::Transaction.as_str() && shard.len() == 2 && !encoded.is_empty())
        .then_some((prefix, shard, encoded))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn collection_id(byte: u8) -> CollectionId {
        CollectionId::from_slice(&[byte; 16]).unwrap()
    }

    #[test]
    fn collection_address_and_key_round_trip() {
        let collection = CollectionAddress::new("db", collection_id(7));
        assert_eq!(collection.db_root(), "db");
        assert_eq!(collection.id(), collection_id(7));
        assert_eq!(
            CollectionAddress::from_physical_prefix(&collection.physical_prefix()).unwrap(),
            collection
        );

        let key = KeyRef::new(collection, b"Hello");
        assert_eq!(key.key(), b"Hello");
        assert_eq!(key.collection().db_root(), "db");
    }

    #[test]
    fn leaf_paths_round_trip() {
        let collection = CollectionAddress::new("db", collection_id(1));
        for leaf in [
            LeafRef::root(collection.clone()),
            LeafRef::node(collection, "token"),
        ] {
            assert_eq!(
                LeafRef::from_physical_path(&leaf.physical_path()).unwrap(),
                leaf
            );
        }
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
        assert_eq!(p, "db/_t/0F/0F8310");
        let r = parse(&p).unwrap();
        assert_eq!(r.typ, Type::Transaction);
        assert_eq!(to_transaction(p.strip_prefix("db/").unwrap()).unwrap(), id);
        assert!(matches!(
            to_transaction("_t/0F8310"),
            Err(PathError::Parse(_))
        ));
        assert_eq!(parse("db/_t/0F8310").unwrap().typ, Type::Unknown);
    }

    #[test]
    fn transaction_prefix_format() {
        assert_eq!(transactions_prefix("db"), "db/_t/");
        assert_eq!(transaction_shard(&TxId::from_bytes(vec![1, 2, 3, 4])), 16);
        assert_eq!(transaction_shard_prefix("db", 16), "db/_t/0F/");
    }

    #[test]
    fn transaction_id_of_round_trip_and_errors() {
        let id = TxId::from_bytes(vec![1, 2, 3, 4]);
        assert_eq!(transaction_id_of(&from_transaction("db", &id)).unwrap(), id);
        assert!(matches!(
            transaction_id_of("db/_t/00/0F8310"),
            Err(PathError::Parse(_))
        ));
        // A non-transaction path is rejected.
        assert!(matches!(
            transaction_id_of(&from_node("db/coll", "node")),
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
            node_token_of(&from_transaction("db", &TxId::from_bytes(vec![1]))),
            Err(PathError::WrongPrefix { .. })
        ));
        // A malformed path (no type segment) is a parse error.
        assert!(matches!(node_token_of("db"), Err(PathError::Parse(_))));
    }

    #[test]
    fn structural_log_record_round_trip() {
        let record_id = "record";
        let path = structural_log_record("db", record_id);
        assert!(path.starts_with(&structural_log_dir("db")));
        assert_eq!(structural_log_id_of(&path).unwrap(), record_id);
        assert_eq!(structural_log_dir("db"), "db/_s/");
        assert_eq!(db_root_of("db/root/child"), "db");
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
        assert_eq!(
            CollectionAddress::root("db").physical_prefix(),
            "db/_c/0000000000000000000000"
        );
        assert_eq!(
            from_transaction("db", &TxId::from_bytes(vec![1, 2, 3, 4])),
            "db/_t/0F/0F8310"
        );
        assert_eq!(collection_info("db/root"), "db/root/_i");
    }
}
