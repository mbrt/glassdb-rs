//! Collection addresses, logical keys, and physical backend-object paths.

use std::sync::Arc;

use crate::base64;
use crate::ids::{CollectionId, ID_BYTES, NodeId, StructuralIntentId, TxId};

mod structural;
mod transaction;
mod tree;

/// Length of the unpadded base64 path component of an ID.
const ID_ENCODED_LEN: usize = (ID_BYTES * 8).div_ceil(6);
const DATABASE_METADATA_OBJECT: &str = "glassdb";

/// A database's top-level physical object-path component.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DbPrefix(Arc<str>);

impl DbPrefix {
    /// Maximum number of bytes in the encoded path component.
    pub const MAX_ENCODED_LEN: usize = 255;

    /// Returns the validated path component.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for DbPrefix {
    type Error = PathError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        validate_db_prefix(value)?;
        Ok(DbPrefix(Arc::from(value)))
    }
}

impl TryFrom<String> for DbPrefix {
    type Error = PathError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_db_prefix(&value)?;
        Ok(DbPrefix(Arc::from(value)))
    }
}

impl std::str::FromStr for DbPrefix {
    type Err = PathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for DbPrefix {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for DbPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn validate_db_prefix(value: &str) -> Result<(), PathError> {
    if value.is_empty()
        || value.len() > DbPrefix::MAX_ENCODED_LEN
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(PathError::InvalidComponent {
            component: "database prefix",
            value: value.to_string(),
        });
    }
    Ok(())
}

/// Parses the path component of a fixed-width ID, such as a node ID.
fn parse_id<T: for<'a> TryFrom<&'a [u8]>>(
    component: &'static str,
    value: &str,
) -> Result<T, PathError> {
    let invalid = || PathError::InvalidComponent {
        component,
        value: value.to_string(),
    };
    if value.len() != ID_ENCODED_LEN {
        return Err(invalid());
    }
    let decoded = base64::decode(value)?;
    // Only the canonical encoding is accepted, so that one ID has one path.
    if base64::encode(&decoded) != value {
        return Err(invalid());
    }
    T::try_from(decoded.as_slice()).map_err(|_| invalid())
}

/// The physical address of one collection within a database.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CollectionAddress {
    db_prefix: DbPrefix,
    id: CollectionId,
}

impl CollectionAddress {
    /// Creates an address from a database prefix and collection identity.
    pub fn new(db_prefix: impl Into<Arc<str>>, id: CollectionId) -> Self {
        let db_prefix = db_prefix.into();
        let db_prefix = DbPrefix::try_from(db_prefix.as_ref())
            .expect("database prefix must be a valid physical path component");
        CollectionAddress { db_prefix, id }
    }

    /// Creates an address from an already validated database prefix.
    pub fn from_db_prefix(db_prefix: DbPrefix, id: CollectionId) -> Self {
        CollectionAddress { db_prefix, id }
    }

    /// Creates the permanent root collection address for `db_prefix`.
    pub fn root(db_prefix: impl Into<Arc<str>>) -> Self {
        Self::new(db_prefix, CollectionId::root())
    }

    /// Returns this address's database prefix.
    pub fn db_prefix(&self) -> &str {
        self.db_prefix.as_str()
    }

    /// Returns the validated database-prefix component.
    pub fn db_prefix_component(&self) -> &DbPrefix {
        &self.db_prefix
    }

    /// Returns this collection's stable collection ID.
    pub fn id(&self) -> CollectionId {
        self.id
    }

    /// Renders the collection prefix used for physical backend objects.
    pub fn physical_prefix(&self) -> String {
        tree::collection_prefix(self.db_prefix.as_str(), self.id)
    }

    /// Parses an ID-addressed physical collection prefix.
    pub fn from_physical_prefix(prefix: &str) -> Result<Self, PathError> {
        let (db_prefix, id) = tree::parse_collection_prefix(prefix)?;
        Ok(CollectionAddress::from_db_prefix(
            DbPrefix::try_from(db_prefix)?,
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
        id: NodeId,
    },
}

impl LeafRef {
    /// Creates a tree-root leaf reference.
    pub fn root(collection: CollectionAddress) -> Self {
        LeafRef::Root(collection)
    }

    /// Creates a standalone-node leaf reference.
    pub fn node(collection: CollectionAddress, id: NodeId) -> Self {
        LeafRef::Node { collection, id }
    }

    /// Returns the collection whose tree contains this leaf.
    pub fn collection(&self) -> &CollectionAddress {
        match self {
            LeafRef::Root(collection) | LeafRef::Node { collection, .. } => collection,
        }
    }

    /// Returns the standalone node ID, or `None` for the tree root.
    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            LeafRef::Root(_) => None,
            LeafRef::Node { id, .. } => Some(*id),
        }
    }

    /// Returns the typed backend object path of this leaf.
    pub fn object_path(&self) -> ObjectPath {
        match self {
            LeafRef::Root(collection) => ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            LeafRef::Node { collection, id } => ObjectPath::Node {
                collection: collection.clone(),
                id: *id,
            },
        }
    }
}

/// A classified physical backend object path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ObjectPath {
    /// The database format and stable-identity record.
    DatabaseMetadata { db_prefix: DbPrefix },
    /// A collection's lifecycle and directory record.
    CollectionRecord { collection: CollectionAddress },
    /// A transaction record, prefix-partitioned by its encoded ID.
    Transaction { db_prefix: DbPrefix, id: TxId },
    /// The fixed root of a collection's B-link tree.
    TreeRoot { collection: CollectionAddress },
    /// A standalone node in a collection's B-link tree.
    Node {
        collection: CollectionAddress,
        id: NodeId,
    },
    /// A participant-owned structural intent.
    StructuralIntent {
        db_prefix: DbPrefix,
        participant: TxId,
        intent_id: StructuralIntentId,
    },
}

impl ObjectPath {
    /// Returns the index of the transaction prefix that contains `id`.
    pub fn transaction_prefix_index(id: &TxId) -> usize {
        transaction::prefix_index(id)
    }

    /// Returns the listing prefix for all standalone nodes in `collection`.
    pub fn nodes_prefix(collection: &CollectionAddress) -> String {
        tree::nodes_prefix(&collection.physical_prefix())
    }

    /// Returns the listing prefix of one transaction prefix index.
    pub fn transaction_prefix(db_prefix: &DbPrefix, index: usize) -> String {
        transaction::index_prefix(db_prefix.as_str(), index)
    }

    /// Returns a transaction listing prefix at one of the three supported depths.
    pub fn transaction_scan_prefix(
        db_prefix: &DbPrefix,
        depth: u8,
        index: usize,
    ) -> Result<String, PathError> {
        transaction::scan_prefix(db_prefix.as_str(), depth, index)
    }

    /// Returns the database-wide structural-intent listing prefix.
    pub fn structural_intents_prefix(db_prefix: &DbPrefix) -> String {
        structural::directory(db_prefix.as_str())
    }

    /// Returns one participant's structural-intent listing prefix.
    pub fn participant_structural_intents_prefix(
        db_prefix: &DbPrefix,
        participant: &TxId,
    ) -> String {
        structural::participant_directory(db_prefix.as_str(), participant)
    }
}

impl std::fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectPath::DatabaseMetadata { db_prefix } => {
                write!(f, "{db_prefix}/{DATABASE_METADATA_OBJECT}")
            }
            ObjectPath::CollectionRecord { collection } => {
                tree::write_collection_record(f, collection)
            }
            ObjectPath::Transaction { db_prefix, id } => {
                transaction::write_object(f, db_prefix.as_str(), id)
            }
            ObjectPath::TreeRoot { collection } => tree::write_tree_root(f, collection),
            ObjectPath::Node { collection, id } => tree::write_node(f, collection, id),
            ObjectPath::StructuralIntent {
                db_prefix,
                participant,
                intent_id,
            } => structural::write_intent(f, db_prefix.as_str(), participant, intent_id),
        }
    }
}

impl TryFrom<&str> for ObjectPath {
    type Error = PathError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        if let Some(db_prefix) = path.strip_suffix("/glassdb") {
            return Ok(ObjectPath::DatabaseMetadata {
                db_prefix: DbPrefix::try_from(db_prefix)?,
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
        CollectionId::from_bytes([byte; ID_BYTES])
    }

    fn node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; ID_BYTES])
    }

    fn intent_id(byte: u8) -> StructuralIntentId {
        StructuralIntentId::from_bytes([byte; ID_BYTES])
    }

    fn tx_id(byte: u8) -> TxId {
        TxId::from_bytes([byte; ID_BYTES])
    }

    #[test]
    fn database_prefix_validation_boundaries() {
        let shortest = DbPrefix::try_from("a").unwrap();
        assert_eq!(shortest.as_str(), "a");
        assert_eq!(shortest.to_string(), "a");

        let longest = "a".repeat(DbPrefix::MAX_ENCODED_LEN);
        assert_eq!(
            DbPrefix::try_from(longest.clone()).unwrap().as_str(),
            longest.as_str()
        );

        assert!(matches!(
            DbPrefix::try_from(""),
            Err(PathError::InvalidComponent { .. })
        ));
        assert!(matches!(
            DbPrefix::try_from("a".repeat(DbPrefix::MAX_ENCODED_LEN + 1)),
            Err(PathError::InvalidComponent { .. })
        ));
        for invalid in ["db-name", "db_name", "db/name", "db name", "dé"] {
            assert!(matches!(
                DbPrefix::try_from(invalid),
                Err(PathError::InvalidComponent { .. })
            ));
        }
    }

    #[test]
    fn id_path_components_require_canonical_128_bit_encodings() {
        let zero = "0000000000000000000000";
        let collection_path = |id: &str| format!("db/_c/{id}/_i");
        let node_path = |id: &str| format!("db/_c/{zero}/_n/{id}");
        let intent_path = |id: &str| format!("db/_s/{zero}/{id}");
        let participant_path = |id: &str| format!("db/_s/{id}/{zero}");
        let transaction_path = |id: &str| {
            let symbols = id.get(..2).unwrap_or("00");
            format!("db/_t/{}/{}/{id}", &symbols[..1], &symbols[1..])
        };
        let parse = |path: String| ObjectPath::try_from(path.as_str());

        assert_eq!(
            parse(collection_path(zero)).unwrap(),
            ObjectPath::CollectionRecord {
                collection: CollectionAddress::root("db"),
            }
        );
        assert_eq!(
            parse(node_path(zero)).unwrap(),
            ObjectPath::Node {
                collection: CollectionAddress::root("db"),
                id: node_id(0),
            }
        );
        assert_eq!(
            parse(intent_path(zero)).unwrap(),
            ObjectPath::StructuralIntent {
                db_prefix: DbPrefix::try_from("db").unwrap(),
                participant: tx_id(0),
                intent_id: intent_id(0),
            }
        );
        assert_eq!(
            parse(transaction_path(zero)).unwrap(),
            ObjectPath::Transaction {
                db_prefix: DbPrefix::try_from("db").unwrap(),
                id: tx_id(0),
            }
        );

        let noncanonical = "0000000000000000000001";
        assert_eq!(
            base64::decode(noncanonical).unwrap(),
            base64::decode(zero).unwrap()
        );
        for invalid in [
            "",
            "000000000000000000000",
            "00000000000000000000000",
            "000000000000000000000!",
            noncanonical,
        ] {
            for path in [
                collection_path(invalid),
                node_path(invalid),
                intent_path(invalid),
                participant_path(invalid),
                transaction_path(invalid),
            ] {
                assert!(parse(path).is_err(), "{invalid:?}");
            }
        }
        for path in [
            collection_path(noncanonical),
            node_path(noncanonical),
            intent_path(noncanonical),
            participant_path(noncanonical),
            transaction_path(noncanonical),
        ] {
            assert!(matches!(
                parse(path),
                Err(PathError::InvalidComponent { .. })
            ));
        }
    }

    #[test]
    fn node_ids_sort_like_their_object_paths() {
        let collection = CollectionAddress::root("db");
        let mut ids = [
            [0xff; ID_BYTES],
            [0x00; ID_BYTES],
            [0x80; ID_BYTES],
            [0x7f; ID_BYTES],
            [0x01; ID_BYTES],
        ]
        .map(NodeId::from_bytes);
        let mut paths = ids.map(|id| {
            ObjectPath::Node {
                collection: collection.clone(),
                id,
            }
            .to_string()
        });
        ids.sort();
        paths.sort();
        assert_eq!(
            ids.map(|id| ObjectPath::Node {
                collection: collection.clone(),
                id,
            }
            .to_string()),
            paths
        );
    }

    #[test]
    fn freshly_minted_ids_round_trip_through_object_paths() {
        let collection = CollectionAddress::root("db");
        let db_prefix = DbPrefix::try_from("db").unwrap();
        let participant = tx_id(3);
        for _ in 0..128 {
            for path in [
                ObjectPath::Node {
                    collection: collection.clone(),
                    id: NodeId::new_random(),
                },
                ObjectPath::StructuralIntent {
                    db_prefix: db_prefix.clone(),
                    participant,
                    intent_id: StructuralIntentId::new_random(),
                },
            ] {
                assert_eq!(
                    ObjectPath::try_from(path.to_string().as_str()).unwrap(),
                    path
                );
            }
        }
    }

    #[test]
    fn every_object_path_variant_round_trips() {
        let db_prefix = DbPrefix::try_from("db").unwrap();
        let collection = CollectionAddress::root("db");
        let id = node_id(7);
        let participant = tx_id(3);
        let intent_id = intent_id(9);
        let paths = [
            ObjectPath::DatabaseMetadata {
                db_prefix: db_prefix.clone(),
            },
            ObjectPath::CollectionRecord {
                collection: collection.clone(),
            },
            ObjectPath::Transaction {
                db_prefix: db_prefix.clone(),
                id: tx_id(1),
            },
            ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            ObjectPath::Node { collection, id },
            ObjectPath::StructuralIntent {
                db_prefix,
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
            InvalidComponent,
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
            ("db/_t/0F/0F8310", ErrorKind::Parse),
            ("db/_t/!/!/!!", ErrorKind::InvalidComponent),
            ("db/_t/!/!/!!!!!!!!!!!!!!!!!!!!!!", ErrorKind::Decode),
            ("db/_t/1/0/0000000000000000000000", ErrorKind::Parse),
            ("db/_t/0/0/é", ErrorKind::InvalidComponent),
            ("db/_t/0/0/€", ErrorKind::InvalidComponent),
            ("db/_s/participant", ErrorKind::Parse),
            ("db/_s//intent", ErrorKind::Parse),
            ("db/_s/!/intent", ErrorKind::InvalidComponent),
            ("db/_s/!!!!!!!!!!!!!!!!!!!!!!/intent", ErrorKind::Decode),
            ("db/_s/participant/intent/extra", ErrorKind::Parse),
        ];

        for (path, expected) in cases {
            let error = ObjectPath::try_from(path).unwrap_err();
            assert!(
                matches!(
                    (expected, error),
                    (ErrorKind::Parse, PathError::Parse(_))
                        | (ErrorKind::Decode, PathError::Decode(_))
                        | (
                            ErrorKind::InvalidComponent,
                            PathError::InvalidComponent { .. }
                        )
                ),
                "unexpected error classification for {path:?}"
            );
        }
    }

    #[test]
    fn family_structure_wins_over_embedded_markers() {
        assert!(matches!(
            ObjectPath::try_from("db/_c/_t/_i"),
            Err(PathError::InvalidComponent { .. })
        ));
        assert!(matches!(
            ObjectPath::try_from("db/_c/0000000000000000000000/_n/_r"),
            Err(PathError::InvalidComponent { .. })
        ));
    }

    #[test]
    fn collection_address_and_key_round_trip() {
        let collection = CollectionAddress::new("db", collection_id(7));
        assert_eq!(collection.db_prefix(), "db");
        assert_eq!(collection.id(), collection_id(7));
        assert_eq!(
            CollectionAddress::from_physical_prefix(&collection.physical_prefix()).unwrap(),
            collection
        );

        let key = LogicalKey::new(collection, b"Hello");
        assert_eq!(key.key(), b"Hello");
        assert_eq!(key.collection().db_prefix(), "db");
    }

    #[test]
    fn leaf_refs_map_to_canonical_object_paths() {
        let collection = CollectionAddress::new("db", collection_id(1));
        let id = node_id(1);
        assert_eq!(
            LeafRef::root(collection.clone()).object_path(),
            ObjectPath::TreeRoot {
                collection: collection.clone(),
            }
        );
        assert_eq!(
            LeafRef::node(collection.clone(), id).object_path(),
            ObjectPath::Node { collection, id }
        );
    }

    // The base64 vectors come from the Go implementation, which uses the same
    // order-preserving alphabet.
    #[test]
    fn path_encoding_golden_vectors() {
        assert_eq!(base64::encode(b"Hello"), "H6KgQ6w");
        assert_eq!(base64::encode(&[0, 1, 2, 3, 4]), "00420kF");
        assert_eq!(base64::encode(b"ab"), "NL8");
        let db_prefix = DbPrefix::try_from("db").unwrap();
        let collection = CollectionAddress::root("db");
        let collection_prefix = "db/_c/0000000000000000000000";
        let id = NodeId::from_bytes([0; ID_BYTES]);
        let participant = TxId::with_priority(0, &[1, 2, 3, 4]);
        let intent_id = StructuralIntentId::from_bytes([0; ID_BYTES]);

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
                id,
            }
            .to_string(),
            format!("{collection_prefix}/_n/0000000000000000000000")
        );
        assert_eq!(
            ObjectPath::Transaction {
                db_prefix: db_prefix.clone(),
                id: participant,
            }
            .to_string(),
            "db/_t/0/F/0F83100000000000000000"
        );
        assert_eq!(
            ObjectPath::StructuralIntent {
                db_prefix: db_prefix.clone(),
                participant,
                intent_id,
            }
            .to_string(),
            "db/_s/0F83100000000000000000/0000000000000000000000"
        );
        assert_eq!(
            ObjectPath::nodes_prefix(&collection),
            format!("{collection_prefix}/_n/")
        );
        assert_eq!(ObjectPath::transaction_prefix(&db_prefix, 16), "db/_t/0/F/");
        assert_eq!(ObjectPath::transaction_prefix_index(&participant), 16);
        assert_eq!(ObjectPath::structural_intents_prefix(&db_prefix), "db/_s/");
        assert_eq!(
            ObjectPath::participant_structural_intents_prefix(&db_prefix, &participant),
            "db/_s/0F83100000000000000000/"
        );
    }
}
