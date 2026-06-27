//! Database metadata version check. Ported from the Go `version.go`.

use std::sync::Arc;

use glassdb_backend::Backend;
use glassdb_data::gopath;

use crate::error::Error;

const DB_VERSION: &str = "v0";
const DB_META_PATH: &str = "glassdb";

/// Verifies the database metadata exists with the expected version, creating it
/// if missing. Races against concurrent creators are resolved by re-checking.
pub(crate) async fn check_or_create_db_meta(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
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

async fn check_db_version(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
    let p = gopath::join(&[name, DB_META_PATH]);
    // The database version lives in the object body (ADR-023): the slimmed
    // backend trait has no object tags.
    let reply = b.read(&p).await?;
    let got = String::from_utf8_lossy(&reply.contents);
    if got != DB_VERSION {
        return Err(Error::internal(format!(
            "got db version {got:?}, expected {DB_VERSION:?}"
        )));
    }
    Ok(())
}

async fn set_db_metadata(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
    let p = gopath::join(&[name, DB_META_PATH]);
    b.write_if_not_exists(&p, DB_VERSION.as_bytes().to_vec())
        .await?;
    Ok(())
}
