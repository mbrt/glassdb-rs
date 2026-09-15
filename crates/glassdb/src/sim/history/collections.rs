//! Public collection operations and their independent sequential specification.

use crate::{Collection, Error, Transaction};

pub(super) const COLLECTION_SLOTS: usize = 2;
const CHILD_NAME: &[u8] = b"nested";
const VALUE_KEY: &[u8] = b"value";

pub(super) type Catalog = [Option<CollectionState>; COLLECTION_SLOTS];

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CollectionState {
    pub(super) value: Option<u8>,
    pub(super) child: Option<Option<u8>>,
}

/// One operation on a collection name shared by every history client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryCollectionOp {
    /// Create a collection, accepting AlreadyExists as an observed result.
    Create,
    /// Create a collection if needed and observe whether it was created.
    CreateIfAbsent,
    /// Create a collection if needed and write its value.
    Write(u8),
    /// Read collection existence, contents, and child membership.
    Read,
    /// Drop a collection, accepting NotFound and NotEmpty as observed results.
    Drop,
    /// Ensure the parent exists and strictly create its child.
    CreateNested,
    /// Ensure the parent and child exist and write the child's value.
    WriteNested(u8),
    /// Drop the child, accepting NotFound as an observed result.
    DropNested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CollectionOutcome {
    Applied,
    Created(bool),
    AlreadyExists,
    NotFound,
    NotEmpty,
}

/// The observations made by one collection instruction within a body execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CollectionStep {
    pub(super) slot: u8,
    pub(super) operation: HistoryCollectionOp,
    pub(super) before: Option<CollectionState>,
    pub(super) after: Option<CollectionState>,
    pub(super) members_before: Vec<u8>,
    pub(super) members_after: Vec<u8>,
    pub(super) outcome: CollectionOutcome,
}

impl CollectionStep {
    pub(super) fn apply(&self, catalog: &mut Catalog) -> Result<(), String> {
        if self.members_before != membership(catalog) {
            return Err(format!(
                "collection listing {:?} disagrees with {catalog:?}",
                self.members_before
            ));
        }
        let state = catalog
            .get_mut(self.slot as usize)
            .ok_or_else(|| format!("invalid collection slot {}", self.slot))?;
        if *state != self.before {
            return Err(format!(
                "collection {} read {:?}, expected {state:?}",
                self.slot, self.before
            ));
        }
        let outcome = self.operation.apply(state);
        if outcome != self.outcome || *state != self.after {
            return Err(format!(
                "collection {} returned {:?} with {:?}, expected {outcome:?} with {state:?}",
                self.slot, self.outcome, self.after
            ));
        }
        if self.members_after != membership(catalog) {
            return Err(format!(
                "collection listing {:?} disagrees with {catalog:?}",
                self.members_after
            ));
        }
        Ok(())
    }
}

impl HistoryCollectionOp {
    fn apply(self, state: &mut Option<CollectionState>) -> CollectionOutcome {
        use CollectionOutcome as O;
        match self {
            Self::Create if state.is_some() => O::AlreadyExists,
            Self::Create => {
                *state = Some(CollectionState::default());
                O::Applied
            }
            Self::CreateIfAbsent => {
                let created = state.is_none();
                state.get_or_insert_with(Default::default);
                O::Created(created)
            }
            Self::Write(value) => {
                state.get_or_insert_with(Default::default).value = Some(value);
                O::Applied
            }
            Self::Read => O::Applied,
            Self::Drop => match state {
                None => O::NotFound,
                Some(collection) if collection.child.is_some() => O::NotEmpty,
                Some(_) => {
                    *state = None;
                    O::Applied
                }
            },
            Self::CreateNested => {
                let parent = state.get_or_insert_with(Default::default);
                if parent.child.is_some() {
                    O::AlreadyExists
                } else {
                    parent.child = Some(None);
                    O::Applied
                }
            }
            Self::WriteNested(value) => {
                state.get_or_insert_with(Default::default).child = Some(Some(value));
                O::Applied
            }
            Self::DropNested => match state {
                Some(parent) if parent.child.is_some() => {
                    parent.child = None;
                    O::Applied
                }
                _ => O::NotFound,
            },
        }
    }
}

pub(super) async fn inspect_catalog(tx: &Transaction) -> Result<Catalog, Error> {
    let mut catalog = Catalog::default();
    let mut listings = Vec::new();
    for (slot, state) in catalog.iter_mut().enumerate() {
        let (observed, listed) = inspect(tx, slot as u8).await?;
        *state = observed;
        listings.push(listed);
    }
    let expected = membership(&catalog);
    crate::ensure_tx!(
        listings.iter().all(|listed| *listed == expected),
        Error::internal("history final catalog disagrees with listing")
    );
    Ok(catalog)
}

pub(super) async fn execute(
    tx: &Transaction,
    slot: u8,
    operation: HistoryCollectionOp,
) -> Result<CollectionStep, Error> {
    let (before, members_before) = inspect(tx, slot).await?;
    let result = execute_operation(tx, slot, operation).await;
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(Error::AlreadyExists) => CollectionOutcome::AlreadyExists,
        Err(Error::NotFound) => CollectionOutcome::NotFound,
        Err(Error::NotEmpty) => CollectionOutcome::NotEmpty,
        Err(error) => return Err(error),
    };
    let (after, members_after) = inspect(tx, slot).await?;
    Ok(CollectionStep {
        slot,
        operation,
        before,
        after,
        members_before,
        members_after,
        outcome,
    })
}

fn collection_name(slot: u8) -> Vec<u8> {
    format!("history-shared-{slot}").into_bytes()
}

fn membership(catalog: &Catalog) -> Vec<u8> {
    catalog
        .iter()
        .enumerate()
        .filter_map(|(slot, state)| state.as_ref().map(|_| slot as u8))
        .collect()
}

async fn read_value(tx: &Transaction, collection: &Collection) -> Result<Option<u8>, Error> {
    match tx.read(collection, VALUE_KEY).await? {
        Some(value) if value.len() == 1 => Ok(Some(value[0])),
        None => Ok(None),
        Some(value) => Err(Error::internal(format!(
            "history collection has invalid value {value:?}"
        ))),
    }
}

async fn inspect(tx: &Transaction, slot: u8) -> Result<(Option<CollectionState>, Vec<u8>), Error> {
    let root = tx.root_collection();
    let name = collection_name(slot);
    let collection = match tx.open_collection(&root, &name).await {
        Ok(collection) => Some(collection),
        Err(Error::NotFound) => None,
        Err(error) => return Err(error),
    };
    let names: Vec<_> = tx
        .iter_collections(&root)
        .await?
        .map(|entry| entry.name)
        .collect();
    let members: Vec<_> = (0..COLLECTION_SLOTS as u8)
        .filter(|slot| names.contains(&collection_name(*slot)))
        .collect();
    let mut expected_names = vec![super::HISTORY_COLLECTION.to_vec()];
    expected_names.extend(members.iter().map(|slot| collection_name(*slot)));
    crate::ensure_tx!(
        names == expected_names && names.contains(&name) == collection.is_some(),
        Error::internal("history collection listing disagrees with lookup")
    );
    let Some(collection) = collection else {
        return Ok((None, members));
    };
    let value = read_value(tx, &collection).await?;
    let child = match tx.open_collection(&collection, CHILD_NAME).await {
        Ok(child) => Some(read_value(tx, &child).await?),
        Err(Error::NotFound) => None,
        Err(error) => return Err(error),
    };
    let children: Vec<_> = tx
        .iter_collections(&collection)
        .await?
        .map(|entry| entry.name)
        .collect();
    let expected: Vec<_> = child.iter().map(|_| CHILD_NAME.to_vec()).collect();
    crate::ensure_tx!(
        children == expected,
        Error::internal("history child listing disagrees with lookup")
    );
    Ok((Some(CollectionState { value, child }), members))
}

async fn execute_operation(
    tx: &Transaction,
    slot: u8,
    operation: HistoryCollectionOp,
) -> Result<CollectionOutcome, Error> {
    use HistoryCollectionOp as C;
    let root = tx.root_collection();
    let name = collection_name(slot);
    match operation {
        C::Read => {}
        C::Create => {
            tx.create_collection(&root, &name).await?;
        }
        C::CreateIfAbsent => {
            let (_, created) = tx.create_collection_if_absent(&root, &name).await?;
            return Ok(CollectionOutcome::Created(created));
        }
        C::Write(value) => {
            let (collection, _) = tx.create_collection_if_absent(&root, &name).await?;
            tx.write(&collection, VALUE_KEY, &[value])?;
        }
        C::Drop => {
            let collection = tx.open_collection(&root, &name).await?;
            tx.drop_collection(&collection).await?;
        }
        C::CreateNested => {
            let (parent, _) = tx.create_collection_if_absent(&root, &name).await?;
            tx.create_collection(&parent, CHILD_NAME).await?;
        }
        C::WriteNested(value) => {
            let (parent, _) = tx.create_collection_if_absent(&root, &name).await?;
            let (child, _) = tx.create_collection_if_absent(&parent, CHILD_NAME).await?;
            tx.write(&child, VALUE_KEY, &[value])?;
        }
        C::DropNested => {
            let parent = tx.open_collection(&root, &name).await?;
            let child = tx.open_collection(&parent, CHILD_NAME).await?;
            tx.drop_collection(&child).await?;
        }
    }
    Ok(CollectionOutcome::Applied)
}
