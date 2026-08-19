//! Database metadata version check. Ported from the Go `version.go`.

use glassdb_backend::Backend;
use glassdb_data::{DATABASE_ID_BYTES, DatabaseId};
use glassdb_proto as pb;
use glassdb_trans::Engine;
use prost::Message;

use crate::error::Error;

// ADR-062 makes point-absence validation incompatible with v2 clients, which
// do not retain the leaf membership generation.
const DB_VERSION: &str = "v3";
const DB_META_PATH: &str = "glassdb";

/// Verifies the database metadata exists with the expected version, creating it
/// if missing. Races against concurrent creators are resolved by re-checking.
pub(crate) async fn check_or_create_db_meta(
    b: &impl Backend,
    name: &str,
) -> Result<DatabaseId, Error> {
    match check_db_version(b, name).await {
        Ok(id) => return Ok(id),
        Err(Error::NotFound) => {}
        Err(e) => return Err(e),
    }
    Engine::prepare_permanent_collection(b, name)
        .await
        .map_err(Error::from)?;
    let proposed = DatabaseId::new_random();
    match set_db_metadata(b, name, proposed).await {
        Ok(()) => Ok(proposed),
        Err(Error::Precondition) => {
            // We raced against another instance; re-check the metadata.
            check_db_version(b, name).await
        }
        Err(e) => Err(Error::with_source("creating db metadata", e)),
    }
}

async fn check_db_version(b: &impl Backend, name: &str) -> Result<DatabaseId, Error> {
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
    let bytes: [u8; DATABASE_ID_BYTES] = meta.database_id.try_into().map_err(|_| {
        Error::internal(format!(
            "database metadata ID must contain exactly {DATABASE_ID_BYTES} bytes"
        ))
    })?;
    Ok(DatabaseId::from_bytes(bytes))
}

async fn set_db_metadata(b: &impl Backend, name: &str, id: DatabaseId) -> Result<(), Error> {
    let p = format!("{name}/{DB_META_PATH}");
    let body = pb::DatabaseMetadata {
        version: DB_VERSION.to_string(),
        database_id: id.as_bytes().to_vec(),
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
        let created = check_or_create_db_meta(&b, "mydb").await.unwrap();
        // Second open decodes the freshly written proto and validates it.
        let reopened = check_or_create_db_meta(&b, "mydb").await.unwrap();
        assert_eq!(created, reopened);
    }

    #[tokio::test]
    async fn arbitrary_16_byte_id_is_accepted() {
        let b = MemoryBackend::new();
        let id = DatabaseId::from_bytes([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ]);
        let body = pb::DatabaseMetadata {
            version: DB_VERSION.to_string(),
            database_id: id.as_bytes().to_vec(),
        }
        .encode_to_vec();
        b.write_if_not_exists(&format!("mydb/{DB_META_PATH}"), body)
            .await
            .unwrap();

        assert_eq!(check_or_create_db_meta(&b, "mydb").await.unwrap(), id);
    }

    #[tokio::test]
    async fn previous_version_is_rejected() {
        let b = MemoryBackend::new();
        let p = format!("mydb/{DB_META_PATH}");
        let body = pb::DatabaseMetadata {
            version: "v2".to_string(),
            database_id: DatabaseId::new_random().as_bytes().to_vec(),
        }
        .encode_to_vec();
        b.write_if_not_exists(&p, body).await.unwrap();

        let err = check_or_create_db_meta(&b, "mydb").await.unwrap_err();
        assert!(
            matches!(&err, Error::Internal { msg, .. } if msg.contains("v2") && msg.contains("v3")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn invalid_id_length_is_rejected() {
        let b = MemoryBackend::new();
        for (name, database_id) in [("missing", vec![]), ("short", vec![0; 15])] {
            let p = format!("{name}/{DB_META_PATH}");
            let body = pb::DatabaseMetadata {
                version: DB_VERSION.to_string(),
                database_id,
            }
            .encode_to_vec();
            b.write_if_not_exists(&p, body).await.unwrap();

            let err = check_or_create_db_meta(&b, name).await.unwrap_err();
            assert!(matches!(err, Error::Internal { .. }), "{name}: got {err:?}");
        }
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
        let id = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let got = pb::DatabaseMetadata {
            version: DB_VERSION.to_string(),
            database_id: id.to_vec(),
        }
        .encode_to_vec();
        // field 1 (string), len 2, "v3"; field 2 (bytes), len 16, ID.
        let mut expected = vec![0x0a, 0x02, 0x76, 0x33, 0x12, 0x10];
        expected.extend(id);
        assert_eq!(got, expected, "db-metadata encoding drifted");
    }
}
