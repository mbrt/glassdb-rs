//! Core data types for GlassDB: transaction identifiers and storage path
//! encoding. Ported from the Go `internal/data` and `internal/data/paths`
//! packages.

pub mod base64;
mod entropy;
pub mod paths;
mod txid;

pub use entropy::shuffle;
pub use paths::{CollectionPath, KeyRef, LeafRef};
pub use txid::{TxId, TxIdSet, set_diff, set_intersect, set_union};
