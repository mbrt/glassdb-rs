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

#[derive(Debug)]
enum ApiCheck {
    EqualBool {
        context: String,
        actual: bool,
        expected: bool,
    },
    EqualValue {
        context: String,
        actual: Option<u8>,
        expected: Option<u8>,
    },
    EqualNames {
        context: String,
        actual: Vec<Vec<u8>>,
        expected: Vec<Vec<u8>>,
    },
    SortedNames {
        context: String,
        names: Vec<Vec<u8>>,
    },
    ValueWidth {
        context: String,
        value: Vec<u8>,
    },
    StaleRead {
        context: String,
        observed: StaleReadObservation,
    },
}

impl ApiCheck {
    fn verify(self) {
        match self {
            ApiCheck::EqualBool {
                context,
                actual,
                expected,
            } => assert_eq!(actual, expected, "{context}"),
            ApiCheck::EqualValue {
                context,
                actual,
                expected,
            } => assert_eq!(actual, expected, "{context}"),
            ApiCheck::EqualNames {
                context,
                actual,
                expected,
            } => assert_eq!(actual, expected, "{context}"),
            ApiCheck::SortedNames { context, names } => assert!(
                names.windows(2).all(|pair| pair[0] < pair[1]),
                "{context}: {names:?}"
            ),
            ApiCheck::ValueWidth { context, value } => {
                assert_eq!(value.len(), 1, "{context} has non-byte value {value:?}");
            }
            ApiCheck::StaleRead { context, observed } => match observed {
                StaleReadObservation::Stale => {}
                StaleReadObservation::Value(value) => {
                    panic!("{context} read {value:?} instead of becoming stale")
                }
            },
        }
    }
}

#[derive(Debug)]
enum StaleReadObservation {
    Stale,
    Value(Option<Vec<u8>>),
}

#[derive(Debug)]
enum KeyReadExpectation {
    Modeled,
    Staged(Option<u8>),
    Repeated(Option<Vec<u8>>),
}

#[derive(Debug)]
struct KeyReadObservation {
    key: usize,
    actual: Option<Vec<u8>>,
    expected: KeyReadExpectation,
}

impl KeyReadObservation {
    fn verify(self, allowed: &[BTreeSet<Option<u8>>]) {
        let actual = modeled_value(&self.actual, format!("API key k{}", self.key));
        match self.expected {
            KeyReadExpectation::Modeled => assert!(
                allowed[self.key].contains(&actual),
                "API key k{} read {actual:?} outside modeled states {:?}",
                self.key,
                allowed[self.key]
            ),
            KeyReadExpectation::Staged(expected) => assert_eq!(
                actual, expected,
                "API key k{} violated read-your-writes",
                self.key
            ),
            KeyReadExpectation::Repeated(expected) => assert_eq!(
                self.actual, expected,
                "API key k{} violated repeatable reads",
                self.key
            ),
        }
    }
}

#[derive(Debug)]
enum CollectionStateObservation {
    Slot {
        slot: usize,
        actual: Option<ApiCollectionModel>,
    },
    Catalog(Vec<Option<ApiCollectionModel>>),
}

#[derive(Debug, Default)]
struct ApiAttempt {
    checks: Vec<ApiCheck>,
    reads: Vec<KeyReadObservation>,
    collections: Vec<CollectionStateObservation>,
}

impl ApiAttempt {
    fn verify(self, allowed: &[BTreeSet<Option<u8>>], after: &BTreeSet<ApiModel>) {
        for check in self.checks {
            check.verify();
        }
        for read in self.reads {
            read.verify(allowed);
        }
        for collection in self.collections {
            match collection {
                CollectionStateObservation::Slot { slot, actual } => {
                    let expected = expected_collection_states(after, slot);
                    assert!(
                        expected.contains(&actual),
                        "collection slot {slot} observed {actual:?} outside modeled states \
                         {expected:?}"
                    );
                }
                CollectionStateObservation::Catalog(actual) => {
                    let expected: BTreeSet<Vec<Option<ApiCollectionModel>>> = after
                        .iter()
                        .map(|model| model.collections.clone())
                        .collect();
                    assert!(
                        expected.contains(&actual),
                        "collection catalog observed {actual:?} outside modeled states {expected:?}"
                    );
                }
            }
        }
    }
}

fn modeled_value(value: &Option<Vec<u8>>, context: impl std::fmt::Display) -> Option<u8> {
    value.as_ref().map(|value| {
        assert_eq!(
            value.len(),
            1,
            "{context} has non-byte modeled value {value:?}"
        );
        value[0]
    })
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
    checks: &mut Vec<ApiCheck>,
    context: String,
) -> Result<Option<u8>, Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await? {
        Some(value) => {
            let modeled = value.first().copied();
            checks.push(ApiCheck::ValueWidth { context, value });
            Ok(modeled)
        }
        None => Ok(None),
    }
}

fn opened_collection(result: Result<Collection, Error>) -> Result<Option<Collection>, Error> {
    match result {
        Ok(collection) => Ok(Some(collection)),
        Err(Error::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn inspect_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
    checks: &mut Vec<ApiCheck>,
) -> Result<Option<ApiCollectionModel>, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let path = api_collection_path(client, slot);
    let exists = tx.collection_exists(&root, &name).await?;
    let path_exists = tx.collection_path_exists(&path).await?;
    checks.push(ApiCheck::EqualBool {
        context: format!("direct and path existence disagree for {name:?}"),
        actual: path_exists,
        expected: exists,
    });

    let root_names = listed_collection_names(tx, &root).await?;
    checks.push(ApiCheck::SortedNames {
        context: "root collection listing is not strictly sorted".into(),
        names: root_names.clone(),
    });
    checks.push(ApiCheck::EqualBool {
        context: format!("root listing disagrees with existence for {name:?}"),
        actual: root_names.iter().any(|candidate| candidate == &name),
        expected: exists,
    });

    let direct_collection = opened_collection(tx.open_collection(&root, &name).await)?;
    let path_collection = opened_collection(tx.open_collection_path(&path).await)?;
    checks.push(ApiCheck::EqualBool {
        context: format!("direct open disagrees with existence for {name:?}"),
        actual: direct_collection.is_some(),
        expected: exists,
    });
    checks.push(ApiCheck::EqualBool {
        context: format!("path open disagrees with existence for {name:?}"),
        actual: path_collection.is_some(),
        expected: path_exists,
    });

    if !exists {
        return Ok(None);
    }

    let Some(collection) = direct_collection.as_ref().or(path_collection.as_ref()) else {
        return Ok(Some(ApiCollectionModel::default()));
    };
    let direct_value = match &direct_collection {
        Some(collection) => Some(
            read_collection_value(tx, collection, checks, "top-level collection".into()).await?,
        ),
        None => None,
    };
    let path_value = match &path_collection {
        Some(collection) => Some(
            read_collection_value(
                tx,
                collection,
                checks,
                "path-opened top-level collection".into(),
            )
            .await?,
        ),
        None => None,
    };
    if let (Some(direct_value), Some(path_value)) = (direct_value, path_value) {
        checks.push(ApiCheck::EqualValue {
            context: format!("direct and path opens disagree for {name:?}"),
            actual: path_value,
            expected: direct_value,
        });
    }
    let value = direct_value.or(path_value).unwrap_or(None);

    let nested_path = api_nested_collection_path(client, slot);
    let child_exists = tx
        .collection_exists(collection, API_NESTED_COLLECTION)
        .await?;
    let path_child_exists = tx.collection_path_exists(&nested_path).await?;
    checks.push(ApiCheck::EqualBool {
        context: format!("direct and path existence disagree for nested child of {name:?}"),
        actual: path_child_exists,
        expected: child_exists,
    });
    let children = listed_collection_names(tx, collection).await?;
    let expected_children = if child_exists {
        vec![API_NESTED_COLLECTION.to_vec()]
    } else {
        Vec::new()
    };
    checks.push(ApiCheck::EqualNames {
        context: format!("nested listing disagrees for {name:?}"),
        actual: children,
        expected: expected_children,
    });

    let direct_child =
        opened_collection(tx.open_collection(collection, API_NESTED_COLLECTION).await)?;
    let path_child = opened_collection(tx.open_collection_path(&nested_path).await)?;
    checks.push(ApiCheck::EqualBool {
        context: format!("direct open disagrees with nested existence for {name:?}"),
        actual: direct_child.is_some(),
        expected: child_exists,
    });
    checks.push(ApiCheck::EqualBool {
        context: format!("path open disagrees with nested existence for {name:?}"),
        actual: path_child.is_some(),
        expected: path_child_exists,
    });

    if !child_exists {
        return Ok(Some(ApiCollectionModel { value, child: None }));
    }

    let direct_child_value = match &direct_child {
        Some(child) => {
            Some(read_collection_value(tx, child, checks, "nested collection".into()).await?)
        }
        None => None,
    };
    let path_child_value = match &path_child {
        Some(child) => Some(
            read_collection_value(tx, child, checks, "path-opened nested collection".into())
                .await?,
        ),
        None => None,
    };
    if let (Some(direct_value), Some(path_value)) = (direct_child_value, path_child_value) {
        checks.push(ApiCheck::EqualValue {
            context: format!("direct and path opens disagree for nested child of {name:?}"),
            actual: path_value,
            expected: direct_value,
        });
    }
    let child_value = direct_child_value.or(path_child_value).unwrap_or(None);

    Ok(Some(ApiCollectionModel {
        value,
        child: Some(ApiChildModel { value: child_value }),
    }))
}

async fn observe_dropped_handle(
    tx: &Transaction,
    collection: &Collection,
    checks: &mut Vec<ApiCheck>,
    context: String,
) -> Result<(), Error> {
    let observed = match tx.read(collection, API_COLLECTION_VALUE_KEY).await {
        Err(Error::StaleCollection) => StaleReadObservation::Stale,
        Err(error) => return Err(error),
        Ok(value) => StaleReadObservation::Value(value),
    };
    checks.push(ApiCheck::StaleRead { context, observed });
    Ok(())
}

async fn ensure_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
    checks: &mut Vec<ApiCheck>,
) -> Result<Collection, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let existed = tx.collection_exists(&root, &name).await?;
    let (collection, created) = tx.create_collection_if_absent(&root, &name).await?;
    checks.push(ApiCheck::EqualBool {
        context: format!("create-if-absent reported the wrong outcome for {name:?}"),
        actual: created,
        expected: !existed,
    });
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

async fn run_read_action(
    tx: &Transaction,
    collection: &Collection,
    key: usize,
    staged: Option<Option<u8>>,
    observed: &mut Option<Option<Vec<u8>>>,
) -> Result<KeyReadObservation, Error> {
    let actual = tx.read(collection, &key_name(key)).await?;
    let expected = if let Some(expected) = staged {
        KeyReadExpectation::Staged(expected)
    } else if let Some(expected) = observed {
        KeyReadExpectation::Repeated(expected.clone())
    } else {
        *observed = Some(actual.clone());
        KeyReadExpectation::Modeled
    };
    Ok(KeyReadObservation {
        key,
        actual,
        expected,
    })
}

async fn run_collection_action(
    tx: &Transaction,
    action: &ApiAction,
    client: usize,
    attempt: &mut ApiAttempt,
) -> Result<(), Error> {
    let root = tx.root_collection();
    match action {
        ApiAction::CreateCollection(slot) => {
            let name = api_collection_name(client, *slot);
            let existed = tx.collection_exists(&root, &name).await?;
            let created = match tx.create_collection(&root, &name).await {
                Ok(_) => true,
                Err(Error::AlreadyExists) => false,
                Err(error) => return Err(error),
            };
            attempt.checks.push(ApiCheck::EqualBool {
                context: format!("strict create reported the wrong outcome for {name:?}"),
                actual: created,
                expected: !existed,
            });
        }
        ApiAction::CreateCollectionIfAbsent(slot) => {
            ensure_collection(tx, client, *slot, &mut attempt.checks).await?;
        }
        ApiAction::ReadCollection(_) | ApiAction::InspectCollections => {}
        ApiAction::WriteCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot, &mut attempt.checks).await?;
            tx.write(&collection, API_COLLECTION_VALUE_KEY, &[*value])?;
            let actual = read_collection_value(
                tx,
                &collection,
                &mut attempt.checks,
                "newly written top-level collection".into(),
            )
            .await?;
            attempt.checks.push(ApiCheck::EqualValue {
                context: "top-level collection violated read-your-writes".into(),
                actual,
                expected: Some(*value),
            });
        }
        ApiAction::CreateNestedCollection(slot) => {
            let collection = ensure_collection(tx, client, *slot, &mut attempt.checks).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let created = match tx
                .create_collection(&collection, API_NESTED_COLLECTION)
                .await
            {
                Ok(_) => true,
                Err(Error::AlreadyExists) => false,
                Err(error) => return Err(error),
            };
            attempt.checks.push(ApiCheck::EqualBool {
                context: "strict nested create reported the wrong outcome".into(),
                actual: created,
                expected: !existed,
            });
        }
        ApiAction::WriteNestedCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot, &mut attempt.checks).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let (child, created) = tx
                .create_collection_if_absent(&collection, API_NESTED_COLLECTION)
                .await?;
            attempt.checks.push(ApiCheck::EqualBool {
                context: "nested create-if-absent reported the wrong outcome".into(),
                actual: created,
                expected: !existed,
            });
            tx.write(&child, API_COLLECTION_VALUE_KEY, &[*value])?;
            let actual = read_collection_value(
                tx,
                &child,
                &mut attempt.checks,
                "newly written nested collection".into(),
            )
            .await?;
            attempt.checks.push(ApiCheck::EqualValue {
                context: "nested collection violated read-your-writes".into(),
                actual,
                expected: Some(*value),
            });
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
                    observe_dropped_handle(
                        tx,
                        &child,
                        &mut attempt.checks,
                        "dropped nested collection handle".into(),
                    )
                    .await?;
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
                let not_empty = match tx.drop_collection(&collection).await {
                    Ok(()) => false,
                    Err(Error::NotEmpty) => true,
                    Err(error) => return Err(error),
                };
                attempt.checks.push(ApiCheck::EqualBool {
                    context: "drop outcome disagrees with nested collection existence".into(),
                    actual: not_empty,
                    expected: child_exists,
                });
                if !not_empty {
                    observe_dropped_handle(
                        tx,
                        &collection,
                        &mut attempt.checks,
                        "dropped top-level collection handle".into(),
                    )
                    .await?;
                }
            }
        }
        ApiAction::Read(_) | ApiAction::Write(_, _) | ApiAction::Delete(_) => {
            return Err(Error::internal(
                "key action routed to collection action executor",
            ));
        }
    }

    if let Some(slot) = collection_slot(action) {
        let actual = inspect_collection(tx, client, slot, &mut attempt.checks).await?;
        attempt
            .collections
            .push(CollectionStateObservation::Slot { slot, actual });
    } else {
        let mut actual = Vec::with_capacity(API_COLLECTION_SLOTS);
        for slot in 0..API_COLLECTION_SLOTS {
            actual.push(inspect_collection(tx, client, slot, &mut attempt.checks).await?);
        }
        attempt
            .collections
            .push(CollectionStateObservation::Catalog(actual));
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
    let client = program.client;
    let result = db
        .tx(|tx| async move {
            let mut staged = [None::<Option<u8>>; API_KEYS];
            let mut observed: [Option<Option<Vec<u8>>>; API_KEYS] = std::array::from_fn(|_| None);
            let mut attempt = ApiAttempt::default();
            for action in actions {
                match action {
                    ApiAction::Read(key) => {
                        attempt.reads.push(
                            run_read_action(
                                &tx,
                                collection,
                                *key,
                                staged[*key],
                                &mut observed[*key],
                            )
                            .await?,
                        );
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
                        run_collection_action(&tx, action, client, &mut attempt).await?;
                    }
                }
            }
            if should_abort {
                tx.abort()?;
            }
            Ok(attempt)
        })
        .await;

    if program.abort {
        return match result {
            Err(Error::Aborted) => Ok(()),
            Ok(_) => panic!("explicitly aborted API transaction committed"),
            Err(error) => Err(error),
        };
    }
    result?.verify(&allowed, &after);
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

        let (root_names, catalogs, checks) = db
            .tx(|tx| async move {
                let root = tx.root_collection();
                let root_names = listed_collection_names(&tx, &root).await?;
                let mut checks = Vec::new();
                let mut catalogs = vec![Vec::with_capacity(API_COLLECTION_SLOTS); nclients];
                for (client, client_catalog) in catalogs.iter_mut().enumerate() {
                    for slot in 0..API_COLLECTION_SLOTS {
                        client_catalog
                            .push(inspect_collection(&tx, client, slot, &mut checks).await?);
                    }
                }
                Ok((root_names, catalogs, checks))
            })
            .await
            .expect("verify final collection catalog");
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
        for check in checks {
            check.verify();
        }
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
