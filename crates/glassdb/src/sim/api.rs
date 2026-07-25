//! Transaction API workload for collection lifecycle, read-your-writes, aborts,
//! and atomicity. Clients own disjoint keys and collection names so every
//! possible in-doubt state can be modeled exactly; they still contend on shared
//! B-link leaves and collection-directory metadata.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;

use crate::{Collection, CollectionPath, Database, Error, Transaction};

use super::harness::{SimWorkload, open_det_db};
use super::{
    MAX_CLIENTS, MAX_OPS_PER_CLIENT, SimMedia, assert_valid_listing, key_name, tiny_split_policy,
};
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
const MAX_ACTIONS_PER_TX: usize = 6;
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

impl<'a> Arbitrary<'a> for ApiWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for client in 0..nclients {
            let owned: Vec<usize> = (0..API_KEYS)
                .filter(|key| key % nclients == client)
                .collect();
            let ntxs = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut txs = Vec::with_capacity(ntxs);
            for _ in 0..ntxs {
                let shape = u.arbitrary::<u8>()?;
                let actions = if shape % 4 == 0 {
                    let slot = u.arbitrary::<u8>()? as usize % API_COLLECTION_SLOTS;
                    let action = match u.arbitrary::<u8>()? % 9 {
                        0 => ApiAction::CreateCollection(slot),
                        1 => ApiAction::CreateCollectionIfAbsent(slot),
                        2 => ApiAction::ReadCollection(slot),
                        3 => ApiAction::WriteCollection(slot, u.arbitrary()?),
                        4 => ApiAction::CreateNestedCollection(slot),
                        5 => ApiAction::WriteNestedCollection(slot, u.arbitrary()?),
                        6 => ApiAction::DropNestedCollection(slot),
                        7 => ApiAction::DropCollection(slot),
                        _ => ApiAction::InspectCollections,
                    };
                    vec![action]
                } else {
                    let nactions = 1 + (shape as usize % MAX_ACTIONS_PER_TX);
                    let mut actions = Vec::with_capacity(nactions);
                    for _ in 0..nactions {
                        let key = owned[u.arbitrary::<u8>()? as usize % owned.len()];
                        actions.push(match u.arbitrary::<u8>()? % 3 {
                            0 => ApiAction::Read(key),
                            1 => ApiAction::Write(key, u.arbitrary()?),
                            _ => ApiAction::Delete(key),
                        });
                    }
                    actions
                };
                txs.push(ApiTransaction {
                    client,
                    actions,
                    abort: u.arbitrary::<u8>()? % 4 == 0,
                });
            }
            clients.push(txs);
        }
        Ok(ApiWorkload { clients })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct ApiChildModel {
    value: Option<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
struct ApiCollectionModel {
    value: Option<u8>,
    child: Option<ApiChildModel>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ApiModel {
    values: Vec<Option<u8>>,
    collections: Vec<Option<ApiCollectionModel>>,
}

impl ApiModel {
    fn new() -> Self {
        Self {
            values: vec![None; API_KEYS],
            collections: vec![None; API_COLLECTION_SLOTS],
        }
    }

    fn apply(&mut self, action: &ApiAction) {
        match action {
            ApiAction::Read(_) | ApiAction::ReadCollection(_) | ApiAction::InspectCollections => {}
            ApiAction::Write(key, value) => self.values[*key] = Some(*value),
            ApiAction::Delete(key) => self.values[*key] = None,
            ApiAction::CreateCollection(slot) | ApiAction::CreateCollectionIfAbsent(slot) => {
                self.collections[*slot].get_or_insert_with(Default::default);
            }
            ApiAction::WriteCollection(slot, value) => {
                self.collections[*slot]
                    .get_or_insert_with(Default::default)
                    .value = Some(*value);
            }
            ApiAction::CreateNestedCollection(slot) => {
                self.collections[*slot]
                    .get_or_insert_with(Default::default)
                    .child
                    .get_or_insert_with(Default::default);
            }
            ApiAction::WriteNestedCollection(slot, value) => {
                self.collections[*slot]
                    .get_or_insert_with(Default::default)
                    .child
                    .get_or_insert_with(Default::default)
                    .value = Some(*value);
            }
            ApiAction::DropNestedCollection(slot) => {
                if let Some(collection) = &mut self.collections[*slot] {
                    collection.child = None;
                }
            }
            ApiAction::DropCollection(slot) => {
                if self.collections[*slot]
                    .as_ref()
                    .is_some_and(|collection| collection.child.is_none())
                {
                    self.collections[*slot] = None;
                }
            }
        }
    }
}

/// Exact reachable states for each client's disjoint key slice.
pub struct ApiAcct {
    possible: Vec<BTreeSet<ApiModel>>,
}

impl ApiAcct {
    fn new(nclients: usize) -> Self {
        let initial = BTreeSet::from([ApiModel::new()]);
        ApiAcct {
            possible: vec![initial; nclients],
        }
    }

    fn apply(model: &ApiModel, program: &ApiTransaction) -> ApiModel {
        let mut next = model.clone();
        for action in &program.actions {
            next.apply(action);
        }
        next
    }

    fn project(before: &BTreeSet<ApiModel>, program: &ApiTransaction) -> BTreeSet<ApiModel> {
        before
            .iter()
            .map(|model| Self::apply(model, program))
            .collect()
    }

    fn begin(&mut self, program: &ApiTransaction) -> (BTreeSet<ApiModel>, BTreeSet<ApiModel>) {
        let before = self.possible[program.client].clone();
        let after = Self::project(&before, program);
        self.possible[program.client].extend(after.iter().cloned());
        (before, after)
    }

    fn confirm(&mut self, client: usize, after: BTreeSet<ApiModel>) {
        self.possible[client] = after;
    }
}

fn possible_values(models: &BTreeSet<ApiModel>, key: usize) -> BTreeSet<Option<u8>> {
    models.iter().map(|model| model.values[key]).collect()
}

/// Marks a body error caused by a value outside the begin-snapshot model.
/// Execution may observe a stale cached value, so OCC validation—not the body—
/// decides whether the attempt can commit.
const OUT_OF_MODEL_MARKER: &str = "api-out-of-model";

fn out_of_model_error(detail: impl std::fmt::Display) -> Error {
    Error::internal(format!("{OUT_OF_MODEL_MARKER}: {detail}"))
}

fn out_of_model_message(error: &Error) -> Option<&str> {
    match error {
        Error::Internal { msg, .. } if msg.starts_with(OUT_OF_MODEL_MARKER) => Some(msg),
        _ => None,
    }
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

fn collection_slot(action: &ApiAction) -> Option<usize> {
    match action {
        ApiAction::CreateCollection(slot)
        | ApiAction::CreateCollectionIfAbsent(slot)
        | ApiAction::ReadCollection(slot)
        | ApiAction::WriteCollection(slot, _)
        | ApiAction::CreateNestedCollection(slot)
        | ApiAction::WriteNestedCollection(slot, _)
        | ApiAction::DropNestedCollection(slot)
        | ApiAction::DropCollection(slot) => Some(*slot),
        ApiAction::Read(_)
        | ApiAction::Write(_, _)
        | ApiAction::Delete(_)
        | ApiAction::InspectCollections => None,
    }
}

async fn listed_collection_names(
    tx: &Transaction,
    parent: &Collection,
) -> Result<Vec<Vec<u8>>, Error> {
    tx.collections(parent)
        .await?
        .map(|entry| entry.map(|entry| entry.name))
        .collect()
}

async fn read_collection_value(
    tx: &Transaction,
    collection: &Collection,
    context: &str,
) -> Result<Option<u8>, Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await? {
        Some(value) => {
            assert_eq!(
                value.len(),
                1,
                "{context} has non-byte modeled value {value:?}"
            );
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
    assert_eq!(
        tx.collection_path_exists(&path).await?,
        exists,
        "direct and path existence disagree for {name:?}"
    );

    let root_names = listed_collection_names(tx, &root).await?;
    assert!(
        root_names.windows(2).all(|pair| pair[0] < pair[1]),
        "root collection listing is not strictly sorted: {root_names:?}"
    );
    assert_eq!(
        root_names.iter().any(|candidate| candidate == &name),
        exists,
        "root listing disagrees with existence for {name:?}"
    );

    if !exists {
        match tx.open_collection(&root, &name).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => panic!("direct open found absent collection {name:?}"),
        }
        match tx.open_collection_path(&path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => panic!("path open found absent collection {name:?}"),
        }
        return Ok(None);
    }

    let collection = tx.open_collection(&root, &name).await?;
    let path_collection = tx.open_collection_path(&path).await?;
    let value = read_collection_value(tx, &collection, "top-level collection").await?;
    assert_eq!(
        read_collection_value(tx, &path_collection, "path-opened top-level collection").await?,
        value,
        "direct and path opens disagree for {name:?}"
    );

    let nested_path = api_nested_collection_path(client, slot);
    let child_exists = tx
        .collection_exists(&collection, API_NESTED_COLLECTION)
        .await?;
    assert_eq!(
        tx.collection_path_exists(&nested_path).await?,
        child_exists,
        "direct and path existence disagree for nested child of {name:?}"
    );
    let children = listed_collection_names(tx, &collection).await?;
    let expected_children = if child_exists {
        vec![API_NESTED_COLLECTION.to_vec()]
    } else {
        Vec::new()
    };
    assert_eq!(
        children, expected_children,
        "nested listing disagrees for {name:?}"
    );

    let child = if child_exists {
        let child = tx
            .open_collection(&collection, API_NESTED_COLLECTION)
            .await?;
        let path_child = tx.open_collection_path(&nested_path).await?;
        let value = read_collection_value(tx, &child, "nested collection").await?;
        assert_eq!(
            read_collection_value(tx, &path_child, "path-opened nested collection").await?,
            value,
            "direct and path opens disagree for nested child of {name:?}"
        );
        Some(ApiChildModel { value })
    } else {
        match tx.open_collection(&collection, API_NESTED_COLLECTION).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => panic!("direct open found absent nested child of {name:?}"),
        }
        match tx.open_collection_path(&nested_path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => panic!("path open found absent nested child of {name:?}"),
        }
        None
    };

    Ok(Some(ApiCollectionModel { value, child }))
}

async fn assert_dropped_handle_is_stale(
    tx: &Transaction,
    collection: &Collection,
) -> Result<(), Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await {
        Err(Error::StaleCollection) => Ok(()),
        Err(error) => Err(error),
        Ok(value) => panic!("dropped collection handle read {value:?} instead of becoming stale"),
    }
}

async fn ensure_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
) -> Result<Collection, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let existed = tx.collection_exists(&root, &name).await?;
    let (collection, created) = tx.create_collection_if_absent(&root, &name).await?;
    assert_eq!(
        created, !existed,
        "create-if-absent reported the wrong outcome for {name:?}"
    );
    Ok(collection)
}

fn expected_collection_states(
    models: &BTreeSet<ApiModel>,
    slot: usize,
) -> BTreeSet<Option<ApiCollectionModel>> {
    models
        .iter()
        .map(|model| model.collections[slot].clone())
        .collect()
}

async fn run_collection_action(
    tx: &Transaction,
    action: &ApiAction,
    client: usize,
    after: &BTreeSet<ApiModel>,
) -> Result<(), Error> {
    let root = tx.root_collection();
    match action {
        ApiAction::CreateCollection(slot) => {
            let name = api_collection_name(client, *slot);
            let existed = tx.collection_exists(&root, &name).await?;
            let result = tx.create_collection(&root, &name).await;
            match (existed, result) {
                (false, Ok(_)) | (true, Err(Error::AlreadyExists)) => {}
                (false, Err(Error::AlreadyExists)) => {
                    panic!("strict create rejected absent collection {name:?}")
                }
                (true, Ok(_)) => panic!("strict create replaced existing collection {name:?}"),
                (_, Err(error)) => return Err(error),
            }
        }
        ApiAction::CreateCollectionIfAbsent(slot) => {
            ensure_collection(tx, client, *slot).await?;
        }
        ApiAction::ReadCollection(_) | ApiAction::InspectCollections => {}
        ApiAction::WriteCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            tx.write(&collection, API_COLLECTION_VALUE_KEY, &[*value])?;
            assert_eq!(
                read_collection_value(tx, &collection, "newly written top-level collection")
                    .await?,
                Some(*value),
                "top-level collection violated read-your-writes"
            );
        }
        ApiAction::CreateNestedCollection(slot) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let result = tx
                .create_collection(&collection, API_NESTED_COLLECTION)
                .await;
            match (existed, result) {
                (false, Ok(_)) | (true, Err(Error::AlreadyExists)) => {}
                (false, Err(Error::AlreadyExists)) => {
                    panic!("strict create rejected an absent nested collection")
                }
                (true, Ok(_)) => panic!("strict create replaced an existing nested collection"),
                (_, Err(error)) => return Err(error),
            }
        }
        ApiAction::WriteNestedCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let (child, created) = tx
                .create_collection_if_absent(&collection, API_NESTED_COLLECTION)
                .await?;
            assert_eq!(
                created, !existed,
                "nested create-if-absent reported the wrong outcome"
            );
            tx.write(&child, API_COLLECTION_VALUE_KEY, &[*value])?;
            assert_eq!(
                read_collection_value(tx, &child, "newly written nested collection").await?,
                Some(*value),
                "nested collection violated read-your-writes"
            );
        }
        ApiAction::DropNestedCollection(slot) => {
            let name = api_collection_name(client, *slot);
            if tx.collection_exists(&root, &name).await? {
                let collection = tx.open_collection(&root, &name).await?;
                if tx
                    .collection_exists(&collection, API_NESTED_COLLECTION)
                    .await?
                {
                    let child = tx
                        .open_collection(&collection, API_NESTED_COLLECTION)
                        .await?;
                    tx.drop_collection(&child).await?;
                    assert_dropped_handle_is_stale(tx, &child).await?;
                }
            }
        }
        ApiAction::DropCollection(slot) => {
            let name = api_collection_name(client, *slot);
            if tx.collection_exists(&root, &name).await? {
                let collection = tx.open_collection(&root, &name).await?;
                let child_exists = tx
                    .collection_exists(&collection, API_NESTED_COLLECTION)
                    .await?;
                match (child_exists, tx.drop_collection(&collection).await) {
                    (true, Err(Error::NotEmpty)) => {}
                    (false, Ok(())) => {
                        assert_dropped_handle_is_stale(tx, &collection).await?;
                    }
                    (true, Ok(())) => panic!("non-recursive drop removed a non-empty collection"),
                    (false, Err(Error::NotEmpty)) => {
                        panic!("drop reported NotEmpty for a childless collection")
                    }
                    (_, Err(error)) => return Err(error),
                }
            }
        }
        ApiAction::Read(_) | ApiAction::Write(_, _) | ApiAction::Delete(_) => {
            unreachable!("key action routed to collection executor")
        }
    }

    if let Some(slot) = collection_slot(action) {
        let actual = inspect_collection(tx, client, slot).await?;
        let allowed = expected_collection_states(after, slot);
        if !allowed.contains(&actual) {
            return Err(out_of_model_error(format!(
                "collection slot {slot} observed {actual:?} outside modeled states {allowed:?}"
            )));
        }
    } else {
        let mut actual = Vec::with_capacity(API_COLLECTION_SLOTS);
        for slot in 0..API_COLLECTION_SLOTS {
            actual.push(inspect_collection(tx, client, slot).await?);
        }
        let allowed: BTreeSet<Vec<Option<ApiCollectionModel>>> = after
            .iter()
            .map(|model| model.collections.clone())
            .collect();
        if !allowed.contains(&actual) {
            return Err(out_of_model_error(format!(
                "collection catalog observed {actual:?} outside modeled states {allowed:?}"
            )));
        }
    }
    Ok(())
}

async fn run_api_program(
    db: &Database,
    program: &ApiTransaction,
    state: &Mutex<ApiAcct>,
) -> Result<(), Error> {
    let (before, after) = if program.abort {
        let before = state.lock().unwrap().possible[program.client].clone();
        let after = ApiAcct::project(&before, program);
        (before, after)
    } else {
        state.lock().unwrap().begin(program)
    };
    let allowed: Vec<BTreeSet<Option<u8>>> = (0..API_KEYS)
        .map(|key| possible_values(&before, key))
        .collect();
    let actions = &program.actions;
    let should_abort = program.abort;
    let collection = db
        .open_collection(&CollectionPath::new(API_COLLECTION)?)
        .await?;
    let collection = &collection;
    let allowed = &allowed;
    let expected_after = &after;
    let result = db
        .tx(|tx| async move {
            let mut staged = [None::<Option<u8>>; API_KEYS];
            let mut observed = [None::<Option<u8>>; API_KEYS];
            for action in actions {
                match action {
                    ApiAction::Read(key) => {
                        let actual = match tx.read(collection, &key_name(*key)).await {
                            Ok(Some(value)) => {
                                assert_eq!(
                                    value.len(),
                                    1,
                                    "API key k{key} has non-byte value {value:?}"
                                );
                                Some(value[0])
                            }
                            Ok(None) => None,
                            Err(error) => return Err(error),
                        };
                        if let Some(expected) = staged[*key] {
                            assert_eq!(
                                actual, expected,
                                "API key k{key} violated read-your-writes"
                            );
                        } else if let Some(expected) = observed[*key] {
                            assert_eq!(
                                actual, expected,
                                "API key k{key} violated repeatable reads"
                            );
                        } else if !allowed[*key].contains(&actual) {
                            return Err(out_of_model_error(format!(
                                "API key k{key} read {actual:?} outside modeled states {:?}",
                                allowed[*key]
                            )));
                        } else {
                            observed[*key] = Some(actual);
                        }
                    }
                    ApiAction::Write(key, value) => {
                        tx.write(collection, &key_name(*key), &[*value])?;
                        staged[*key] = Some(Some(*value));
                    }
                    ApiAction::Delete(key) => {
                        tx.delete(collection, &key_name(*key))?;
                        staged[*key] = Some(None);
                    }
                    ApiAction::CreateCollection(_)
                    | ApiAction::CreateCollectionIfAbsent(_)
                    | ApiAction::ReadCollection(_)
                    | ApiAction::WriteCollection(_, _)
                    | ApiAction::CreateNestedCollection(_)
                    | ApiAction::WriteNestedCollection(_, _)
                    | ApiAction::DropNestedCollection(_)
                    | ApiAction::DropCollection(_)
                    | ApiAction::InspectCollections => {
                        run_collection_action(&tx, action, program.client, expected_after).await?;
                    }
                }
            }
            if should_abort { tx.abort() } else { Ok(()) }
        })
        .await;

    // A merely stale value causes the engine to retry the body. If this marker
    // escapes, OCC accepted an out-of-model snapshot as current.
    if let Err(error) = &result
        && let Some(message) = out_of_model_message(error)
    {
        panic!("{message}");
    }

    if program.abort {
        return match result {
            Err(Error::Aborted) => Ok(()),
            Ok(()) => panic!("explicitly aborted API transaction committed"),
            Err(error) => Err(error),
        };
    }
    result?;
    state.lock().unwrap().confirm(program.client, after);
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
        open_det_db(backend, tiny_split_policy(), media).await
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
        run_api_program(db, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Mutex<ApiAcct>, _failures_enabled: bool) {
        let collection = db
            .open_collection(&CollectionPath::new(API_COLLECTION).unwrap())
            .await
            .expect("open API collection");
        let listed: Vec<Vec<u8>> = collection
            .keys()
            .await
            .expect("final API listing")
            .collect::<Result<_, _>>()
            .expect("final API listing");
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
            actual[key % nclients].values[key] = value;
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
                assert!(
                    root_names.iter().all(|name| known_names.contains(name)),
                    "root listing contains an unmodeled collection: {root_names:?}"
                );

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
            model.collections = catalog;
        }

        let acct = state.lock().unwrap();
        for (client, actual) in actual.iter().enumerate() {
            assert!(
                acct.possible[client].contains(actual),
                "client {client} final API state {:?} is not reachable; expected one of {:?}",
                actual,
                acct.possible[client]
            );
        }
    }
}
