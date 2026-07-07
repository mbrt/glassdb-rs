//! Database metadata version check. Ported from the Go `version.go`.

use glassdb_backend::Backend;
use glassdb_proto as pb;
use prost::Message;

use crate::error::Error;

// Bumped to "v2" with the v2 engine redesign, which broke on-disk
// compatibility: a database written by an older format must be rejected rather
// than silently misread.
const DB_VERSION: &str = "v2";
const DB_META_PATH: &str = "glassdb";

/// Verifies the database metadata exists with the expected version, creating it
/// if missing. Races against concurrent creators are resolved by re-checking.
pub(crate) async fn check_or_create_db_meta(b: &impl Backend, name: &str) -> Result<(), Error> {
    match check_db_version(b, name).await {
        Ok(()) => return Ok(()),
        Err(Error::NotFound) => {}
        Err(e) => return Err(e),
    }
    match set_db_metadata(b, name).await {
        Ok(()) => Ok(()),
        Err(Error::Precondition) => {
            // We raced against another instance; re-check the metadata.
            check_db_version(b, name).await
        }
        Err(e) => Err(Error::with_source("creating db metadata", e)),
    }
}

async fn check_db_version(b: &impl Backend, name: &str) -> Result<(), Error> {
    let p = format!("{name}/{DB_META_PATH}");
    // The metadata lives in the object body (ADR-023): the slimmed backend
    // trait has no object tags. It is an evolvable protobuf message so new
    // fields can be added without breaking existing databases.
    let reply = b.read(&p).await?;
    let meta = pb::DatabaseMetadata::decode(reply.contents.as_slice())
        .map_err(|e| Error::with_source("decoding db metadata", e))?;
    if meta.version != DB_VERSION {
        return Err(Error::internal(format!(
            "got db version {:?}, expected {DB_VERSION:?}",
            meta.version
        )));
    }
    Ok(())
}

async fn set_db_metadata(b: &impl Backend, name: &str) -> Result<(), Error> {
    let p = format!("{name}/{DB_META_PATH}");
    let body = pb::DatabaseMetadata {
        version: DB_VERSION.to_string(),
    }
    .encode_to_vec();
    b.write_if_not_exists(&p, body).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::memory::MemoryBackend;

    #[tokio::test]
    async fn create_then_validate_round_trips_through_proto() {
        let b = MemoryBackend::new();
        // First open creates the metadata object.
        check_or_create_db_meta(&b, "mydb").await.unwrap();
        // Second open decodes the freshly written proto and validates it.
        check_or_create_db_meta(&b, "mydb").await.unwrap();
    }

    #[tokio::test]
    async fn wrong_version_is_rejected() {
        let b = MemoryBackend::new();
        let p = format!("mydb/{DB_META_PATH}");
        let body = pb::DatabaseMetadata {
            version: "v1".to_string(),
        }
        .encode_to_vec();
        b.write_if_not_exists(&p, body).await.unwrap();

        let err = check_or_create_db_meta(&b, "mydb").await.unwrap_err();
        assert!(
            matches!(&err, Error::Internal { msg, .. } if msg.contains("v1") && msg.contains("v2")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn legacy_naked_string_body_is_rejected() {
        // A pre-v2 database stored the version as a naked string, which is not a
        // valid protobuf message; opening it must fail rather than misread.
        let b = MemoryBackend::new();
        let p = format!("mydb/{DB_META_PATH}");
        b.write_if_not_exists(&p, b"v0".to_vec()).await.unwrap();

        let err = check_or_create_db_meta(&b, "mydb").await.unwrap_err();
        assert!(matches!(err, Error::Internal { .. }), "got {err:?}");
    }

    // Golden vector: the metadata body must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let got = pb::DatabaseMetadata {
            version: DB_VERSION.to_string(),
        }
        .encode_to_vec();
        // field 1 (string), len 2, "v2".
        assert_eq!(
            got,
            [0x0a, 0x02, 0x76, 0x32],
            "db-metadata encoding drifted"
        );
    }
}
