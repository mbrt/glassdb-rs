//! Core data types for GlassDB: transaction identities and storage path
//! encoding. Ported from the Go `internal/data` and `internal/data/paths`
//! packages.

pub mod base64;
mod entropy;
mod ids;
mod names;
mod paths;

pub use entropy::shuffle;
pub use ids::{CollectionId, DatabaseId, ID_BYTES, NodeId, StructuralIntentId, TxId};
pub use names::{CollectionName, InvalidCollectionName, MAX_COLLECTION_NAME_BYTES};
pub use paths::{CollectionAddress, DbPrefix, LeafRef, LogicalKey, ObjectPath, PathError};
