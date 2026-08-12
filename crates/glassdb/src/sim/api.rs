//! Transaction API workload for collection lifecycle, read-your-writes, aborts,
//! and atomicity. Clients own disjoint keys and collection names so every
//! possible in-doubt state can be modeled exactly; they still contend on shared
//! B-link leaves and collection-directory metadata.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use glassdb_backend::Backend;

use crate::{Collection, CollectionPath, Database, Error, Transaction};

mod executor;
mod generator;
mod model;

pub use model::ApiAcct;

use super::harness::{SimWorkload, open_det_db};
use super::{SimMedia, assert_valid_listing, key_name, tiny_split_policy};
use executor::StepResult;
use model::{ApiChildModel, ApiCollectionModel, ApiModel};
// ===========================================================================
// Transaction API workload (inspired by FoundationDB FuzzApiCorrectness).
//
// Each operation is either a small key transaction program or one collection
// lifecycle program, optionally ending in an explicit abort. Keeping one
// lifecycle action per generated transaction avoids constructing combinations
// that the public API deliberately rejects (such as drop-after-write) while the
// action itself can still compose collection and data changes atomically.
// Clients own disjoint keys and collection names, letting the harness retain
// both outcomes of every in-doubt commit and verify an exact reachable state.
// ===========================================================================

const API_KEYS: usize = 8;
const API_COLLECTION: &[u8] = b"api";
const API_COLLECTION_SLOTS: usize = 2;
const API_NESTED_COLLECTION: &[u8] = b"nested";
const API_COLLECTION_VALUE_KEY: &[u8] = b"value";

/// One public transaction API call in an [`ApiTransaction`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiAction {
    /// Read a key and check read-your-writes and repeatable-read behavior.
    Read(usize),
    /// Stage a one-byte value for a key.
    Write(usize, u8),
    /// Stage a key deletion.
    Delete(usize),
    /// Strictly create a client-owned top-level collection.
    CreateCollection(usize),
    /// Idempotently create a client-owned top-level collection.
    CreateCollectionIfAbsent(usize),
    /// Read and cross-check a collection through direct and path APIs.
    ReadCollection(usize),
    /// Create a collection if needed and atomically write its modeled value.
    WriteCollection(usize, u8),
    /// Ensure a nested child exists using strict creation when possible.
    CreateNestedCollection(usize),
    /// Ensure a nested child exists and atomically write its modeled value.
    WriteNestedCollection(usize, u8),
    /// Drop a nested child when it exists.
    DropNestedCollection(usize),
    /// Drop a top-level collection, expecting `NotEmpty` while its child exists.
    DropCollection(usize),
    /// Cross-check every client-owned binding through existence and listing APIs.
    InspectCollections,
}

/// A sequence of API calls executed atomically by one client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiTransaction {
    /// Owning client. Its keys are the residue class `key % client_count`.
    pub client: usize,
    /// Calls made in order within one transaction.
    pub actions: Vec<ApiAction>,
    /// Whether the transaction explicitly aborts after running its calls.
    pub abort: bool,
}

/// Random transaction programs executed by concurrent clients.
#[derive(Debug, Clone)]
pub struct ApiWorkload {
    /// Per-client transaction sequences.
    pub clients: Vec<Vec<ApiTransaction>>,
}

impl Default for ApiWorkload {
    fn default() -> Self {
        ApiWorkload {
            clients: vec![Vec::new(), Vec::new()],
        }
    }
}

/// Marks an oracle failure that must first pass transaction read validation.
const API_INVARIANT_MARKER: &str = "api-invariant";

fn api_invariant_error(detail: impl std::fmt::Display) -> Error {
    Error::internal(format!("{API_INVARIANT_MARKER}: {detail}"))
}

fn api_invariant_message(error: &Error) -> Option<&str> {
    match error {
        Error::Internal { msg, .. } if msg.starts_with(API_INVARIANT_MARKER) => Some(msg),
        _ => None,
    }
}

fn check_api_invariant(condition: bool, detail: impl std::fmt::Display) -> Result<(), Error> {
    crate::ensure_tx!(condition, api_invariant_error(detail));
    Ok(())
}

fn api_collection_name(client: usize, slot: usize) -> Vec<u8> {
    format!("api-c{client}-{slot}").into_bytes()
}

fn api_collection_path(client: usize, slot: usize) -> CollectionPath {
    CollectionPath::new(api_collection_name(client, slot)).expect("valid API collection name")
}

fn api_nested_collection_path(client: usize, slot: usize) -> CollectionPath {
    api_collection_path(client, slot)
        .child(API_NESTED_COLLECTION)
        .expect("valid nested API collection name")
}

async fn listed_collection_names(
    tx: &Transaction,
    parent: &Collection,
) -> Result<Vec<Vec<u8>>, Error> {
    Ok(tx
        .iter_collections(parent)
        .await?
        .map(|entry| entry.name)
        .collect())
}

async fn read_collection_value(
    tx: &Transaction,
    collection: &Collection,
    context: &str,
) -> Result<Option<u8>, Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await? {
        Some(value) => {
            check_api_invariant(
                value.len() == 1,
                format!("{context} has non-byte modeled value {value:?}"),
            )?;
            Ok(Some(value[0]))
        }
        None => Ok(None),
    }
}

async fn inspect_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
) -> Result<Option<ApiCollectionModel>, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let path = api_collection_path(client, slot);
    let exists = tx.collection_exists(&root, &name).await?;
    let path_exists = tx.collection_path_exists(&path).await?;
    check_api_invariant(
        path_exists == exists,
        format!("direct and path existence disagree for {name:?}"),
    )?;

    let root_names = listed_collection_names(tx, &root).await?;
    check_api_invariant(
        root_names.windows(2).all(|pair| pair[0] < pair[1]),
        format!("root collection listing is not strictly sorted: {root_names:?}"),
    )?;
    check_api_invariant(
        root_names.iter().any(|candidate| candidate == &name) == exists,
        format!("root listing disagrees with existence for {name:?}"),
    )?;

    if !exists {
        match tx.open_collection(&root, &name).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "direct open found absent collection {name:?}"
                )));
            }
        }
        match tx.open_collection_path(&path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "path open found absent collection {name:?}"
                )));
            }
        }
        return Ok(None);
    }

    let collection = tx.open_collection(&root, &name).await?;
    let path_collection = tx.open_collection_path(&path).await?;
    let value = read_collection_value(tx, &collection, "top-level collection").await?;
    let path_value =
        read_collection_value(tx, &path_collection, "path-opened top-level collection").await?;
    check_api_invariant(
        path_value == value,
        format!("direct and path opens disagree for {name:?}"),
    )?;

    let nested_path = api_nested_collection_path(client, slot);
    let child_exists = tx
        .collection_exists(&collection, API_NESTED_COLLECTION)
        .await?;
    let path_child_exists = tx.collection_path_exists(&nested_path).await?;
    check_api_invariant(
        path_child_exists == child_exists,
        format!("direct and path existence disagree for nested child of {name:?}"),
    )?;
    let children = listed_collection_names(tx, &collection).await?;
    let expected_children = if child_exists {
        vec![API_NESTED_COLLECTION.to_vec()]
    } else {
        Vec::new()
    };
    check_api_invariant(
        children == expected_children,
        format!("nested listing disagrees for {name:?}"),
    )?;

    let child = if child_exists {
        let child = tx
            .open_collection(&collection, API_NESTED_COLLECTION)
            .await?;
        let path_child = tx.open_collection_path(&nested_path).await?;
        let value = read_collection_value(tx, &child, "nested collection").await?;
        let path_value =
            read_collection_value(tx, &path_child, "path-opened nested collection").await?;
        check_api_invariant(
            path_value == value,
            format!("direct and path opens disagree for nested child of {name:?}"),
        )?;
        Some(ApiChildModel::new(value))
    } else {
        match tx.open_collection(&collection, API_NESTED_COLLECTION).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "direct open found absent nested child of {name:?}"
                )));
            }
        }
        match tx.open_collection_path(&nested_path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "path open found absent nested child of {name:?}"
                )));
            }
        }
        None
    };

    Ok(Some(ApiCollectionModel::new(value, child)))
}

async fn run_api_step(
    db: &Database,
    program: &ApiTransaction,
    state: &Mutex<ApiAcct>,
) -> Result<(), Error> {
    let (before, after) = if program.abort {
        let before = state.lock().unwrap().possible(program.client).clone();
        let after = ApiAcct::project(&before, program);
        (before, after)
    } else {
        state.lock().unwrap().begin(program)
    };

    match executor::execute_step(db, program, &before, &after).await? {
        StepResult::Committed => state.lock().unwrap().confirm(program.client, after),
        StepResult::ExplicitlyAborted => {}
    }
    Ok(())
}

impl SimWorkload for ApiWorkload {
    type Op = ApiTransaction;
    type State = Mutex<ApiAcct>;

    fn clients(&self) -> &[Vec<ApiTransaction>] {
        &self.clients
    }

    fn new_state(&self) -> Mutex<ApiAcct> {
        Mutex::new(ApiAcct::new(self.clients.len()))
    }

    async fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> Result<Database, Error> {
        open_det_db(
            backend,
            tiny_split_policy(),
            glassdb_storage::InlinePolicy::default(),
            media,
        )
        .await
    }

    async fn seed(&self, db: &Database) {
        db.root_collection()
            .create_collection_if_absent(API_COLLECTION)
            .await
            .expect("create API collection");
    }

    async fn run_op(
        db: &Database,
        op: &ApiTransaction,
        state: &Mutex<ApiAcct>,
    ) -> Result<(), Error> {
        run_api_step(db, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Mutex<ApiAcct>, _failures_enabled: bool) {
        let collection = db
            .open_collection(&CollectionPath::new(API_COLLECTION).unwrap())
            .await
            .expect("open API collection");
        let listed: Vec<Vec<u8>> = collection
            .iter_keys()
            .await
            .expect("final API listing")
            .collect();
        assert_valid_listing(&listed, API_KEYS);

        let nclients = self.clients.len();
        let mut actual = vec![ApiModel::new(); nclients];
        for key in 0..API_KEYS {
            let name = key_name(key);
            let value = match collection.read(&name).await {
                Ok(Some(value)) => {
                    assert_eq!(
                        value.len(),
                        1,
                        "API key k{key} has non-byte value {value:?}"
                    );
                    assert!(
                        listed.contains(&name),
                        "API key k{key} readable but not listed"
                    );
                    Some(value[0])
                }
                Ok(None) => {
                    assert!(
                        !listed.contains(&name),
                        "API key k{key} listed but not readable"
                    );
                    None
                }
                Err(error) => panic!("final API read failed for k{key}: {error}"),
            };
            actual[key % nclients].set_value(key, value);
        }

        let catalogs = db
            .tx(|tx| async move {
                let root = tx.root_collection();
                let root_names = listed_collection_names(&tx, &root).await?;
                let mut known_names = BTreeSet::from([API_COLLECTION.to_vec()]);
                for client in 0..nclients {
                    for slot in 0..API_COLLECTION_SLOTS {
                        known_names.insert(api_collection_name(client, slot));
                    }
                }
                check_api_invariant(
                    root_names.iter().all(|name| known_names.contains(name)),
                    format!("root listing contains an unmodeled collection: {root_names:?}"),
                )?;

                let mut catalogs = vec![Vec::with_capacity(API_COLLECTION_SLOTS); nclients];
                for (client, client_catalog) in catalogs.iter_mut().enumerate() {
                    for slot in 0..API_COLLECTION_SLOTS {
                        client_catalog.push(inspect_collection(&tx, client, slot).await?);
                    }
                }
                Ok(catalogs)
            })
            .await
            .expect("verify final collection catalog");
        for (model, catalog) in actual.iter_mut().zip(catalogs) {
            model.set_collections(catalog);
        }

        let acct = state.lock().unwrap();
        for (client, actual) in actual.iter().enumerate() {
            assert!(
                acct.contains(client, actual),
                "client {client} final API state {:?} is not reachable; expected one of {:?}",
                actual,
                acct.possible(client)
            );
        }
    }
}
