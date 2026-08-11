//! Collection/key references and physical backend-object paths.

use std::sync::Arc;

use crate::base64;
use crate::collection_id::CollectionId;
use crate::txid::TxId;

mod structural;
mod transaction;
mod tree;

const NODE_TOKEN_BYTES: usize = 16;
const DATABASE_METADATA_OBJECT: &str = "glassdb";

/// A database's top-level physical object-path component.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbRoot(Arc<str>);

impl DbRoot {
    /// Maximum number of bytes in the encoded path component.
    pub const MAX_ENCODED_LEN: usize = 255;

    /// Returns the validated path component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for DbRoot {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_db_root(value)?;
        Ok(DbRoot(Arc::from(value)))
    }
}

impl TryFrom<String> for DbRoot {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_db_root(&value)?;
        Ok(DbRoot(Arc::from(value)))
    }
}

impl std::str::FromStr for DbRoot {
    type Err = PathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for DbRoot {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for DbRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The canonical random identity component of a non-root B-link node.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeToken(Arc<str>);

impl NodeToken {
    /// Maximum number of bytes in the encoded path component.
    pub const MAX_ENCODED_LEN: usize = 22;

    /// Mints a fresh random node token.
    ///
    /// Random bytes precede encoding so object-store partitions see a
    /// high-entropy prefix. The entropy source is simulation-aware.
    pub fn new_random() -> Self {
        let mut bytes = [0u8; NODE_TOKEN_BYTES];
        crate::entropy::fill_random(&mut bytes);
        Self::from_bytes(bytes)
    }

    /// Encodes an exact 128-bit node identity.
    pub fn from_bytes(bytes: [u8; NODE_TOKEN_BYTES]) -> Self {
        NodeToken(Arc::from(base64::encode(&bytes)))
    }

    /// Returns the canonical encoded path component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for NodeToken {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_random_component("node token", value, Self::MAX_ENCODED_LEN)?;
        Ok(NodeToken(Arc::from(value)))
    }
}

impl TryFrom<String> for NodeToken {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_random_component("node token", &value, Self::MAX_ENCODED_LEN)?;
        Ok(NodeToken(Arc::from(value)))
    }
}

impl std::str::FromStr for NodeToken {
    type Err = PathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for NodeToken {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for NodeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The canonical random identity component of a structural-log record.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructuralRecordId(Arc<str>);

impl StructuralRecordId {
    /// Maximum number of bytes in the encoded path component.
    pub const MAX_ENCODED_LEN: usize = NodeToken::MAX_ENCODED_LEN;

    /// Returns the canonical encoded path component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for StructuralRecordId {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_random_component(
            "structural record ID",
            value,
            StructuralRecordId::MAX_ENCODED_LEN,
        )?;
        Ok(StructuralRecordId(Arc::from(value)))
    }
}

impl TryFrom<String> for StructuralRecordId {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_random_component(
            "structural record ID",
            &value,
            StructuralRecordId::MAX_ENCODED_LEN,
        )?;
        Ok(StructuralRecordId(Arc::from(value)))
    }
}

impl std::str::FromStr for StructuralRecordId {
    type Err = PathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl From<NodeToken> for StructuralRecordId {
    fn from(value: NodeToken) -> Self {
        StructuralRecordId(value.0)
    }
}

impl From<&NodeToken> for StructuralRecordId {
    fn from(value: &NodeToken) -> Self {
        StructuralRecordId(value.0.clone())
    }
}

impl AsRef<str> for StructuralRecordId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for StructuralRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_db_root(value: &str) -> Result<(), PathError> {
    if value.is_empty()
        || value.len() > DbRoot::MAX_ENCODED_LEN
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(PathError::InvalidComponent {
            component: "database root",
            value: value.to_string(),
        });
    }
    Ok(())
}

fn validate_random_component(
    component: &'static str,
    value: &str,
    encoded_len: usize,
) -> Result<(), PathError> {
    if value.len() != encoded_len {
        return Err(PathError::InvalidComponent {
            component,
            value: value.to_string(),
        });
    }
    let decoded = base64::decode(value)?;
    if decoded.len() != NODE_TOKEN_BYTES || base64::encode(&decoded) != value {
        return Err(PathError::InvalidComponent {
            component,
            value: value.to_string(),
        });
    }
    Ok(())
}

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
        tree::collection_prefix(&self.db_root, self.id)
    }

    /// Parses an incarnation-addressed physical collection prefix.
    pub fn from_physical_prefix(prefix: &str) -> Result<Self, PathError> {
        let (db_root, id) = tree::parse_collection_prefix(prefix)?;
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
            LeafRef::Root(collection) => tree::tree_root(&collection.physical_prefix()),
            LeafRef::Node { collection, token } => tree::node(&collection.physical_prefix(), token),
        }
    }

    /// Parses a physical collection-root or node path.
    pub fn from_physical_path(path: &str) -> Result<Self, PathError> {
        match ObjectPath::try_from(path) {
            Ok(ObjectPath::TreeRoot { collection }) => Ok(LeafRef::root(collection)),
            Ok(ObjectPath::Node { collection, token }) => {
                Ok(LeafRef::node(collection, token.to_string()))
            }
            Ok(_) => Err(PathError::Parse(path.to_string())),
            Err(_) => legacy_leaf_from_physical_path(path),
        }
    }
}

/// Number of deterministic transaction-log shards (two base64 symbols).
pub const TRANSACTION_SHARD_COUNT: usize = 64 * 64;

/// A classified physical backend object path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectPath {
    /// The database format and stable-identity record.
    DatabaseMetadata { db_root: DbRoot },
    /// A collection's lifecycle and directory record.
    CollectionRecord { collection: CollectionAddress },
    /// A transaction log, deterministically sharded by its encoded ID.
    Transaction { db_root: DbRoot, id: TxId },
    /// The fixed root of a collection's B-link tree.
    TreeRoot { collection: CollectionAddress },
    /// A standalone node in a collection's B-link tree.
    Node {
        collection: CollectionAddress,
        token: NodeToken,
    },
    /// A participant-owned structural-log record.
    StructuralRecord {
        db_root: DbRoot,
        participant: TxId,
        record_id: StructuralRecordId,
    },
}

impl std::fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectPath::DatabaseMetadata { db_root } => {
                write!(f, "{db_root}/{DATABASE_METADATA_OBJECT}")
            }
            ObjectPath::CollectionRecord { collection } => {
                tree::write_collection_record(f, &collection.physical_prefix())
            }
            ObjectPath::Transaction { db_root, id } => {
                transaction::write_object(f, db_root.as_str(), id)
            }
            ObjectPath::TreeRoot { collection } => {
                tree::write_tree_root(f, &collection.physical_prefix())
            }
            ObjectPath::Node { collection, token } => {
                tree::write_node(f, &collection.physical_prefix(), token.as_str())
            }
            ObjectPath::StructuralRecord {
                db_root,
                participant,
                record_id,
            } => structural::write_record(f, db_root.as_str(), participant, record_id.as_str()),
        }
    }
}

impl TryFrom<&str> for ObjectPath {
    type Error = PathError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        if let Some(db_root) = path.strip_suffix("/glassdb") {
            return Ok(ObjectPath::DatabaseMetadata {
                db_root: DbRoot::try_from(db_root)?,
            });
        }
        if let Some(result) = transaction::parse_object(path) {
            return result;
        }
        if let Some(result) = structural::parse_object(path) {
            return result;
        }
        if let Some(result) = tree::parse_object(path) {
            return result;
        }
        Err(PathError::Parse(path.to_string()))
    }
}

impl std::str::FromStr for ObjectPath {
    type Err = PathError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        Self::try_from(path)
    }
}

/// The category of a storage path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Unknown,
    Transaction,
    CollectionRecord,
    /// The fixed B-link tree root object (`_r`, ADR-050).
    TreeRoot,
    /// A B-link tree node object (`_n/<token>`, ADR-031).
    Node,
}

impl Type {
    /// Returns the physical object marker (`_t`, `_i`, `_r`, `_n`, or `""`).
    pub fn as_str(self) -> &'static str {
        match self {
            Type::Unknown => "",
            Type::Transaction => "_t",
            Type::CollectionRecord => "_i",
            Type::TreeRoot => "_r",
            Type::Node => "_n",
        }
    }
}

/// Error returned by path parsing/decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    /// The path did not have the expected `prefix/type/suffix` structure.
    Parse(String),
    /// One typed path component violated its canonical representation.
    InvalidComponent {
        component: &'static str,
        value: String,
    },
    /// The suffix did not start with the expected type marker.
    WrongPrefix { suffix: String, expected: String },
    /// The base64 payload could not be decoded.
    Decode(base64::DecodeError),
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathError::Parse(p) => write!(f, "expected path with >=3 parts, got {p:?}"),
            PathError::InvalidComponent { component, value } => {
                write!(f, "invalid {component} path component {value:?}")
            }
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

/// Returns the storage path for the collection record under `prefix`.
pub fn collection_record(prefix: &str) -> String {
    tree::collection_record(prefix)
}

/// Reports whether `p` refers to a collection record.
pub fn is_collection_record(p: &str) -> bool {
    match ObjectPath::try_from(p) {
        Ok(ObjectPath::CollectionRecord { .. }) => true,
        Ok(_) => false,
        Err(_) => p.ends_with("/_i"),
    }
}

/// Returns the storage path for the fixed B-link tree root under `prefix`.
pub fn tree_root(prefix: &str) -> String {
    tree::tree_root(prefix)
}

/// Reports whether `p` refers to a fixed B-link tree root object.
pub fn is_tree_root(p: &str) -> bool {
    match ObjectPath::try_from(p) {
        Ok(ObjectPath::TreeRoot { .. }) => true,
        Ok(_) => false,
        Err(_) => p.ends_with("/_r"),
    }
}

/// Encodes a transaction ID into a storage path under `prefix`.
pub fn from_transaction(prefix: &str, id: &TxId) -> String {
    transaction::object(prefix, id)
}

/// Decodes a transaction ID from a sharded storage path suffix
/// (`_t/<shard>/<b64>`).
pub fn to_transaction(suffix: &str) -> Result<TxId, PathError> {
    transaction::parse_suffix(suffix)
}

/// Returns the listing prefix for all transaction objects under `prefix`.
pub fn transactions_prefix(prefix: &str) -> String {
    transaction::prefix(prefix)
}

/// Returns the deterministic shard index for `id`.
pub fn transaction_shard(id: &TxId) -> usize {
    transaction::shard(id)
}

/// Returns the listing prefix for one deterministic transaction-log shard.
pub fn transaction_shard_prefix(prefix: &str, shard: usize) -> String {
    transaction::shard_prefix(prefix, shard)
}

/// Decodes the transaction ID from a full transaction object path
/// (`{prefix}/_t/<shard>/<b64>`), the inverse of [`from_transaction`]. Unlike
/// [`to_transaction`] (which decodes a type-marked suffix), this takes a whole
/// path as returned by a transaction listing.
pub fn transaction_id_of(path: &str) -> Result<TxId, PathError> {
    match transaction::parse_object(path) {
        Some(Ok(ObjectPath::Transaction { id, .. })) => Ok(id),
        Some(Ok(_)) => unreachable!("transaction codec returned a non-transaction path"),
        Some(Err(error)) => Err(error),
        None if path_parts_indexes(path).is_none() => Err(PathError::Parse(path.to_string())),
        None => Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: format!("{}/<shard>/<txid>", Type::Transaction.as_str()),
        }),
    }
}

/// Returns the storage path for the B-link node named `token` under `prefix`
/// (`{prefix}/_n/<token>`, ADR-031).
///
/// The token is an opaque identity string (typically from [`random_node_token`]),
/// not a computed index: the tree is dynamic, so a node is addressed by
/// descending to it, never by formula.
pub fn from_node(prefix: &str, token: &str) -> String {
    tree::node(prefix, token)
}

/// Returns the listing prefix for all B-link node objects under `prefix`.
pub fn nodes_prefix(prefix: &str) -> String {
    tree::nodes_prefix(prefix)
}

/// Returns the database-wide structural-log directory (`{db}/_s/`).
pub fn structural_log_dir(db_root: &str) -> String {
    structural::directory(db_root)
}

/// Returns one topology participant's structural-log directory
/// (`{db}/_s/<participant_id>/`).
pub fn structural_log_participant_dir(db_root: &str, participant: &TxId) -> String {
    structural::participant_directory(db_root, participant)
}

/// Returns the path of one participant-owned structural-log record
/// (`{db}/_s/<participant_id>/<record_id>`).
pub fn structural_log_record(db_root: &str, participant: &TxId, record_id: &str) -> String {
    structural::record(db_root, participant, record_id)
}

/// Decodes the participant and record id from a structural-log record path.
pub fn structural_log_parts_of(path: &str) -> Result<(TxId, String), PathError> {
    match ObjectPath::try_from(path) {
        Ok(ObjectPath::StructuralRecord {
            participant,
            record_id,
            ..
        }) => Ok((participant, record_id.to_string())),
        Ok(_) => Err(PathError::Parse(path.to_string())),
        Err(_) => structural::parse_legacy(path),
    }
}

/// Decodes the record id from a structural-log record path.
pub fn structural_log_id_of(path: &str) -> Result<String, PathError> {
    structural_log_parts_of(path).map(|(_, record_id)| record_id)
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
    match ObjectPath::try_from(path) {
        Ok(ObjectPath::Node { token, .. }) => Ok(token.to_string()),
        Ok(_) => Err(PathError::WrongPrefix {
            suffix: path.to_string(),
            expected: Type::Node.as_str().to_string(),
        }),
        Err(_) => {
            let parsed = legacy_parse(path)?;
            if parsed.typ == Type::Node {
                Ok(parsed.suffix)
            } else {
                Err(PathError::WrongPrefix {
                    suffix: path.to_string(),
                    expected: Type::Node.as_str().to_string(),
                })
            }
        }
    }
}

/// Mints a fresh, random B-link node token.
///
/// The token is deliberately random rather than monotonic: object stores
/// partition by key prefix, so monotonically increasing names would pile new
/// nodes onto one partition and accidentally hot-key the backend (ADR-031). It
/// draws from the same seeded-under-simulation entropy as [`crate::TxId`], so
/// DST replays stay byte-identical.
pub fn random_node_token() -> String {
    NodeToken::new_random().to_string()
}

/// Splits a storage path into its prefix, type, and suffix components.
pub fn parse(p: &str) -> Result<ParseResult, PathError> {
    let Ok(path) = ObjectPath::try_from(p) else {
        return legacy_parse(p);
    };
    Ok(match path {
        ObjectPath::DatabaseMetadata { db_root } => ParseResult {
            prefix: db_root.to_string(),
            suffix: DATABASE_METADATA_OBJECT.to_string(),
            typ: Type::Unknown,
        },
        ObjectPath::CollectionRecord { collection } => ParseResult {
            prefix: collection.physical_prefix(),
            suffix: String::new(),
            typ: Type::CollectionRecord,
        },
        ObjectPath::Transaction { db_root, id } => ParseResult {
            prefix: db_root.to_string(),
            suffix: transaction::encoded_id(&id),
            typ: Type::Transaction,
        },
        ObjectPath::TreeRoot { collection } => ParseResult {
            prefix: collection.physical_prefix(),
            suffix: String::new(),
            typ: Type::TreeRoot,
        },
        ObjectPath::Node { collection, token } => ParseResult {
            prefix: collection.physical_prefix(),
            suffix: token.to_string(),
            typ: Type::Node,
        },
        ObjectPath::StructuralRecord {
            db_root, record_id, ..
        } => ParseResult {
            prefix: format!("{db_root}/_s"),
            suffix: record_id.to_string(),
            typ: Type::Unknown,
        },
    })
}

fn legacy_leaf_from_physical_path(path: &str) -> Result<LeafRef, PathError> {
    if let Some(prefix) = path.strip_suffix("/_r") {
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

fn legacy_parse(p: &str) -> Result<ParseResult, PathError> {
    if let Some(prefix) = p.strip_suffix("/_i") {
        return Ok(ParseResult {
            prefix: prefix.to_string(),
            suffix: String::new(),
            typ: Type::CollectionRecord,
        });
    }
    if let Some(prefix) = p.strip_suffix("/_r") {
        return Ok(ParseResult {
            prefix: prefix.to_string(),
            suffix: String::new(),
            typ: Type::TreeRoot,
        });
    }
    if let Some((prefix, shard, suffix)) = legacy_transaction_parts(p)
        && shard == suffix.get(..2).unwrap_or("00")
    {
        return Ok(ParseResult {
            prefix: prefix.to_string(),
            suffix: suffix.to_string(),
            typ: Type::Transaction,
        });
    }
    let (prefix_idx, type_idx) =
        path_parts_indexes(p).ok_or_else(|| PathError::Parse(p.to_string()))?;
    let marker = &p[prefix_idx + 1..type_idx];
    Ok(ParseResult {
        prefix: p[..prefix_idx].to_string(),
        suffix: p[type_idx + 1..].to_string(),
        typ: if marker == Type::Node.as_str() {
            Type::Node
        } else {
            Type::Unknown
        },
    })
}

fn legacy_transaction_parts(path: &str) -> Option<(&str, &str, &str)> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn collection_id(byte: u8) -> CollectionId {
        CollectionId::from_slice(&[byte; 16]).unwrap()
    }

    #[test]
    fn database_root_validation_boundaries() {
        let shortest = DbRoot::try_from("a").unwrap();
        assert_eq!(shortest.as_str(), "a");
        assert_eq!(shortest.to_string(), "a");

        let longest = "a".repeat(DbRoot::MAX_ENCODED_LEN);
        assert_eq!(
            DbRoot::try_from(longest.clone()).unwrap().as_str(),
            longest.as_str()
        );

        assert!(matches!(
            DbRoot::try_from(""),
            Err(PathError::InvalidComponent { .. })
        ));
        assert!(matches!(
            DbRoot::try_from("a".repeat(DbRoot::MAX_ENCODED_LEN + 1)),
            Err(PathError::InvalidComponent { .. })
        ));
        for invalid in ["db-name", "db_name", "db/name", "db name", "dé"] {
            assert!(matches!(
                DbRoot::try_from(invalid),
                Err(PathError::InvalidComponent { .. })
            ));
        }
    }

    #[test]
    fn random_identity_components_require_canonical_128_bit_encodings() {
        let token = NodeToken::from_bytes([0; NODE_TOKEN_BYTES]);
        assert_eq!(token.as_str(), "0000000000000000000000");
        assert_eq!(token.as_str().len(), NodeToken::MAX_ENCODED_LEN);
        assert_eq!(NodeToken::try_from(token.to_string()).unwrap(), token);
        assert_eq!(token.as_str().parse::<NodeToken>().unwrap(), token);

        let record_id = StructuralRecordId::try_from(token.as_str()).unwrap();
        assert_eq!(record_id.as_str(), token.as_str());
        assert_eq!(StructuralRecordId::from(&token), record_id);
        assert_eq!(StructuralRecordId::from(token.clone()), record_id);

        for invalid in ["", "000000000000000000000", "00000000000000000000000"] {
            assert!(NodeToken::try_from(invalid).is_err());
            assert!(StructuralRecordId::try_from(invalid).is_err());
        }

        let invalid_alphabet = "000000000000000000000!";
        assert!(NodeToken::try_from(invalid_alphabet).is_err());
        assert!(StructuralRecordId::try_from(invalid_alphabet).is_err());

        let noncanonical = "0000000000000000000001";
        assert_eq!(
            base64::decode(noncanonical).unwrap(),
            base64::decode(token.as_str()).unwrap()
        );
        assert!(matches!(
            NodeToken::try_from(noncanonical),
            Err(PathError::InvalidComponent { .. })
        ));
        assert!(matches!(
            StructuralRecordId::try_from(noncanonical),
            Err(PathError::InvalidComponent { .. })
        ));
    }

    #[test]
    fn freshly_minted_node_tokens_round_trip() {
        for _ in 0..128 {
            let token = NodeToken::new_random();
            assert_eq!(token.as_str().len(), NodeToken::MAX_ENCODED_LEN);
            assert_eq!(NodeToken::try_from(token.to_string()).unwrap(), token);
            assert_eq!(base64::decode(token.as_str()).unwrap().len(), 16);
        }
    }

    #[test]
    fn every_object_path_variant_round_trips() {
        let db_root = DbRoot::try_from("db").unwrap();
        let collection = CollectionAddress::root("db");
        let token = NodeToken::from_bytes([7; NODE_TOKEN_BYTES]);
        let participant = TxId::from_bytes(b"participant".to_vec());
        let record_id = StructuralRecordId::from(NodeToken::from_bytes([9; NODE_TOKEN_BYTES]));
        let paths = [
            ObjectPath::DatabaseMetadata {
                db_root: db_root.clone(),
            },
            ObjectPath::CollectionRecord {
                collection: collection.clone(),
            },
            ObjectPath::Transaction {
                db_root: db_root.clone(),
                id: TxId::from_bytes(vec![1, 2, 3, 4]),
            },
            ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            ObjectPath::Node { collection, token },
            ObjectPath::StructuralRecord {
                db_root,
                participant,
                record_id,
            },
        ];

        for path in paths {
            let encoded = path.to_string();
            assert_eq!(ObjectPath::try_from(encoded.as_str()).unwrap(), path);
            assert_eq!(encoded.parse::<ObjectPath>().unwrap(), path);
        }
    }

    #[test]
    fn object_path_error_classification_is_stable() {
        enum ErrorKind {
            Parse,
            Decode,
        }

        let cases = [
            ("db", ErrorKind::Parse),
            ("/unknown/object", ErrorKind::Parse),
            ("db//object", ErrorKind::Parse),
            ("db/unknown/", ErrorKind::Parse),
            ("/_r", ErrorKind::Parse),
            ("db/_n/", ErrorKind::Parse),
            ("db/_n/token/extra", ErrorKind::Parse),
            ("db/_t/00/0F8310", ErrorKind::Parse),
            ("db/_t/!!/!!", ErrorKind::Decode),
            ("db/_s/participant", ErrorKind::Parse),
            ("db/_s//record", ErrorKind::Parse),
            ("db/_s/!/record", ErrorKind::Decode),
            ("db/_s/participant/record/extra", ErrorKind::Parse),
        ];

        for (path, expected) in cases {
            let error = ObjectPath::try_from(path).unwrap_err();
            assert!(
                matches!(
                    (expected, error),
                    (ErrorKind::Parse, PathError::Parse(_))
                        | (ErrorKind::Decode, PathError::Decode(_))
                ),
                "unexpected error classification for {path:?}"
            );
        }
    }

    #[test]
    fn family_structure_wins_over_embedded_markers() {
        assert!(matches!(
            ObjectPath::try_from("db/_c/_t/_i"),
            Err(PathError::Parse(_))
        ));
        assert!(matches!(
            ObjectPath::try_from("db/_c/0000000000000000000000/_n/_r"),
            Err(PathError::InvalidComponent { .. })
        ));
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
    fn collection_record_paths() {
        assert_eq!(collection_record("foo/bar"), "foo/bar/_i");
        assert!(is_collection_record("foo/bar/_i"));
        let r = parse("foo/bar/_i").unwrap();
        assert_eq!(r.prefix, "foo/bar");
        assert_eq!(r.typ, Type::CollectionRecord);
    }

    #[test]
    fn tree_root_paths() {
        assert_eq!(tree_root("foo/bar"), "foo/bar/_r");
        assert!(is_tree_root("foo/bar/_r"));
        let parsed = parse("foo/bar/_r").unwrap();
        assert_eq!(parsed.prefix, "foo/bar");
        assert_eq!(parsed.typ, Type::TreeRoot);
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
        let participant = TxId::from_bytes(b"participant".to_vec());
        let record_id = "record";
        let path = structural_log_record("db", &participant, record_id);
        assert!(path.starts_with(&structural_log_dir("db")));
        assert!(path.starts_with(&structural_log_participant_dir("db", &participant)));
        assert_eq!(
            structural_log_parts_of(&path).unwrap(),
            (participant, record_id.to_string())
        );
        assert_eq!(structural_log_id_of(&path).unwrap(), record_id);
        assert_eq!(structural_log_dir("db"), "db/_s/");
        assert_eq!(db_root_of("db/root/child"), "db");
        assert!(structural_log_parts_of("db/_s/record").is_err());
        assert!(structural_log_parts_of("db/_s//record").is_err());
        assert!(structural_log_parts_of("db/_s/participant/").is_err());
        assert!(structural_log_parts_of("db/_s/participant/record/extra").is_err());
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
        assert_eq!(collection_record("db/root"), "db/root/_i");
        assert_eq!(tree_root("db/root"), "db/root/_r");
    }
}
