//! Collection addresses, logical keys, and physical backend-object paths.

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
        glassdb_concurr::entropy::fill_bytes(&mut bytes);
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

/// The canonical random identity component of a structural intent.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StructuralIntentId(Arc<str>);

impl StructuralIntentId {
    /// Maximum number of bytes in the encoded path component.
    pub const MAX_ENCODED_LEN: usize = NodeToken::MAX_ENCODED_LEN;

    /// Returns the canonical encoded path component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for StructuralIntentId {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_random_component(
            "structural intent ID",
            value,
            StructuralIntentId::MAX_ENCODED_LEN,
        )?;
        Ok(StructuralIntentId(Arc::from(value)))
    }
}

impl TryFrom<String> for StructuralIntentId {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_random_component(
            "structural intent ID",
            &value,
            StructuralIntentId::MAX_ENCODED_LEN,
        )?;
        Ok(StructuralIntentId(Arc::from(value)))
    }
}

impl std::str::FromStr for StructuralIntentId {
    type Err = PathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl From<NodeToken> for StructuralIntentId {
    fn from(value: NodeToken) -> Self {
        StructuralIntentId(value.0)
    }
}

impl From<&NodeToken> for StructuralIntentId {
    fn from(value: &NodeToken) -> Self {
        StructuralIntentId(value.0.clone())
    }
}

impl AsRef<str> for StructuralIntentId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for StructuralIntentId {
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
    db_root: DbRoot,
    id: CollectionId,
}

impl CollectionAddress {
    /// Creates an address from a database root and collection identity.
    pub fn new(db_root: impl Into<Arc<str>>, id: CollectionId) -> Self {
        let db_root = db_root.into();
        let db_root = DbRoot::try_from(db_root.as_ref())
            .expect("database root must be a valid physical path component");
        CollectionAddress { db_root, id }
    }

    /// Creates an address from an already validated database root.
    pub fn from_db_root(db_root: DbRoot, id: CollectionId) -> Self {
        CollectionAddress { db_root, id }
    }

    /// Creates the permanent root collection address for `db_root`.
    pub fn root(db_root: impl Into<Arc<str>>) -> Self {
        Self::new(db_root, CollectionId::root())
    }

    /// Returns this address's database root.
    pub fn db_root(&self) -> &str {
        self.db_root.as_str()
    }

    /// Returns the validated database-root component.
    pub fn db_root_component(&self) -> &DbRoot {
        &self.db_root
    }

    /// Returns this collection's stable incarnation identity.
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Renders the collection prefix used for physical backend objects.
    pub fn physical_prefix(&self) -> String {
        tree::collection_prefix(self.db_root.as_str(), self.id)
    }

    /// Parses an incarnation-addressed physical collection prefix.
    pub fn from_physical_prefix(prefix: &str) -> Result<Self, PathError> {
        let (db_root, id) = tree::parse_collection_prefix(prefix)?;
        Ok(CollectionAddress::from_db_root(
            DbRoot::try_from(db_root)?,
            id,
        ))
    }
}

/// A logical key stored inside a collection leaf.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalKey {
    collection: CollectionAddress,
    key: Arc<[u8]>,
}

impl LogicalKey {
    /// Creates a logical key.
    pub fn new(collection: CollectionAddress, key: impl AsRef<[u8]>) -> Self {
        LogicalKey {
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
        token: NodeToken,
    },
}

impl LeafRef {
    /// Creates a collection-root leaf reference.
    pub fn root(collection: CollectionAddress) -> Self {
        LeafRef::Root(collection)
    }

    /// Creates a standalone-node leaf reference.
    pub fn node(collection: CollectionAddress, token: NodeToken) -> Self {
        LeafRef::Node { collection, token }
    }

    /// Returns the collection whose tree contains this leaf.
    pub fn collection(&self) -> &CollectionAddress {
        match self {
            LeafRef::Root(collection) | LeafRef::Node { collection, .. } => collection,
        }
    }

    /// Returns the standalone node token, or `None` for the collection root.
    pub fn node_token(&self) -> Option<&NodeToken> {
        match self {
            LeafRef::Root(_) => None,
            LeafRef::Node { token, .. } => Some(token),
        }
    }

    /// Returns the typed backend object path of this leaf.
    pub fn object_path(&self) -> ObjectPath {
        match self {
            LeafRef::Root(collection) => ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            LeafRef::Node { collection, token } => ObjectPath::Node {
                collection: collection.clone(),
                token: token.clone(),
            },
        }
    }
}

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
    /// A participant-owned structural intent.
    StructuralIntent {
        db_root: DbRoot,
        participant: TxId,
        intent_id: StructuralIntentId,
    },
}

impl ObjectPath {
    /// Returns the deterministic transaction-log shard containing `id`.
    pub fn transaction_shard(id: &TxId) -> usize {
        transaction::shard(id)
    }

    /// Returns the listing prefix for all standalone nodes in `collection`.
    pub fn nodes_prefix(collection: &CollectionAddress) -> String {
        tree::nodes_prefix(&collection.physical_prefix())
    }

    /// Returns the listing prefix for one deterministic transaction shard.
    pub fn transaction_shard_prefix(db_root: &DbRoot, shard: usize) -> String {
        transaction::shard_prefix(db_root.as_str(), shard)
    }

    /// Returns the database-wide structural-intent listing prefix.
    pub fn structural_intents_prefix(db_root: &DbRoot) -> String {
        structural::directory(db_root.as_str())
    }

    /// Returns one participant's structural-intent listing prefix.
    pub fn participant_structural_intents_prefix(db_root: &DbRoot, participant: &TxId) -> String {
        structural::participant_directory(db_root.as_str(), participant)
    }
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
            ObjectPath::StructuralIntent {
                db_root,
                participant,
                intent_id,
            } => structural::write_intent(f, db_root.as_str(), participant, intent_id.as_str()),
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

        let intent_id = StructuralIntentId::try_from(token.as_str()).unwrap();
        assert_eq!(intent_id.as_str(), token.as_str());
        assert_eq!(StructuralIntentId::from(&token), intent_id);
        assert_eq!(StructuralIntentId::from(token.clone()), intent_id);

        for invalid in ["", "000000000000000000000", "00000000000000000000000"] {
            assert!(NodeToken::try_from(invalid).is_err());
            assert!(StructuralIntentId::try_from(invalid).is_err());
        }

        let invalid_alphabet = "000000000000000000000!";
        assert!(NodeToken::try_from(invalid_alphabet).is_err());
        assert!(StructuralIntentId::try_from(invalid_alphabet).is_err());

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
            StructuralIntentId::try_from(noncanonical),
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
        let intent_id = StructuralIntentId::from(NodeToken::from_bytes([9; NODE_TOKEN_BYTES]));
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
            ObjectPath::StructuralIntent {
                db_root,
                participant,
                intent_id,
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
            ("db/_s//intent", ErrorKind::Parse),
            ("db/_s/!/intent", ErrorKind::Decode),
            ("db/_s/participant/intent/extra", ErrorKind::Parse),
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

        let key = LogicalKey::new(collection, b"Hello");
        assert_eq!(key.key(), b"Hello");
        assert_eq!(key.collection().db_root(), "db");
    }

    #[test]
    fn leaf_refs_map_to_canonical_object_paths() {
        let collection = CollectionAddress::new("db", collection_id(1));
        let token = NodeToken::from_bytes([1; NODE_TOKEN_BYTES]);
        assert_eq!(
            LeafRef::root(collection.clone()).object_path(),
            ObjectPath::TreeRoot {
                collection: collection.clone(),
            }
        );
        assert_eq!(
            LeafRef::node(collection.clone(), token.clone()).object_path(),
            ObjectPath::Node { collection, token }
        );
    }

    // Golden vectors produced by the Go implementation, to guarantee
    // byte-for-byte compatibility of the path encoding.
    #[test]
    fn golden_vectors_match_go() {
        assert_eq!(base64::encode(b"Hello"), "H6KgQ6w");
        assert_eq!(base64::encode(&[0, 1, 2, 3, 4]), "00420kF");
        assert_eq!(base64::encode(b"ab"), "NL8");
        let db_root = DbRoot::try_from("db").unwrap();
        let collection = CollectionAddress::root("db");
        let collection_prefix = "db/_c/0000000000000000000000";
        let token = NodeToken::from_bytes([0; NODE_TOKEN_BYTES]);
        let participant = TxId::from_bytes(vec![1, 2, 3, 4]);
        let intent_id = StructuralIntentId::from(token.clone());

        assert_eq!(collection.physical_prefix(), collection_prefix);
        assert_eq!(
            ObjectPath::CollectionRecord {
                collection: collection.clone(),
            }
            .to_string(),
            format!("{collection_prefix}/_i")
        );
        assert_eq!(
            ObjectPath::TreeRoot {
                collection: collection.clone(),
            }
            .to_string(),
            format!("{collection_prefix}/_r")
        );
        assert_eq!(
            ObjectPath::Node {
                collection: collection.clone(),
                token,
            }
            .to_string(),
            format!("{collection_prefix}/_n/0000000000000000000000")
        );
        assert_eq!(
            ObjectPath::Transaction {
                db_root: db_root.clone(),
                id: participant.clone(),
            }
            .to_string(),
            "db/_t/0F/0F8310"
        );
        assert_eq!(
            ObjectPath::StructuralIntent {
                db_root: db_root.clone(),
                participant: participant.clone(),
                intent_id,
            }
            .to_string(),
            "db/_s/0F8310/0000000000000000000000"
        );
        assert_eq!(
            ObjectPath::nodes_prefix(&collection),
            format!("{collection_prefix}/_n/")
        );
        assert_eq!(
            ObjectPath::transaction_shard_prefix(&db_root, 16),
            "db/_t/0F/"
        );
        assert_eq!(ObjectPath::transaction_shard(&participant), 16);
        assert_eq!(ObjectPath::structural_intents_prefix(&db_root), "db/_s/");
        assert_eq!(
            ObjectPath::participant_structural_intents_prefix(&db_root, &participant),
            "db/_s/0F8310/"
        );
    }
}
