//! Pure exact-state model for transaction API programs.

use std::collections::BTreeSet;

use super::{API_COLLECTION_SLOTS, API_KEYS, ApiAction, ApiTransaction};

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ApiChildModel {
    value: Option<u8>,
}

impl ApiChildModel {
    pub(super) fn new(value: Option<u8>) -> Self {
        Self { value }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ApiCollectionModel {
    value: Option<u8>,
    child: Option<ApiChildModel>,
}

impl ApiCollectionModel {
    pub(super) fn new(value: Option<u8>, child: Option<ApiChildModel>) -> Self {
        Self { value, child }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct ApiModel {
    values: Vec<Option<u8>>,
    collections: Vec<Option<ApiCollectionModel>>,
}

impl ApiModel {
    pub(super) fn new() -> Self {
        Self {
            values: vec![None; API_KEYS],
            collections: vec![None; API_COLLECTION_SLOTS],
        }
    }

    pub(super) fn set_value(&mut self, key: usize, value: Option<u8>) {
        self.values[key] = value;
    }

    pub(super) fn set_collections(&mut self, collections: Vec<Option<ApiCollectionModel>>) {
        self.collections = collections;
    }

    fn apply(&mut self, action: &ApiAction) -> Result<(), ApiTransitionError> {
        match action {
            ApiAction::Read(_) | ApiAction::ReadCollection(_) | ApiAction::InspectCollections => {
                Ok(())
            }
            ApiAction::Write(key, value) => {
                self.values[*key] = Some(*value);
                Ok(())
            }
            ApiAction::Delete(key) => {
                self.values[*key] = None;
                Ok(())
            }
            ApiAction::CreateCollection(slot) => {
                if self.collections[*slot].is_some() {
                    Err(ApiTransitionError::AlreadyExists)
                } else {
                    self.collections[*slot] = Some(ApiCollectionModel::default());
                    Ok(())
                }
            }
            ApiAction::CreateCollectionIfAbsent(slot) => {
                self.collections[*slot].get_or_insert_with(Default::default);
                Ok(())
            }
            ApiAction::WriteCollection(slot, value) => {
                self.collections[*slot]
                    .get_or_insert_with(Default::default)
                    .value = Some(*value);
                Ok(())
            }
            ApiAction::CreateNestedCollection(slot) => {
                let collection = self.collections[*slot].get_or_insert_with(Default::default);
                if collection.child.is_some() {
                    Err(ApiTransitionError::AlreadyExists)
                } else {
                    collection.child = Some(ApiChildModel::default());
                    Ok(())
                }
            }
            ApiAction::WriteNestedCollection(slot, value) => {
                self.collections[*slot]
                    .get_or_insert_with(Default::default)
                    .child
                    .get_or_insert_with(Default::default)
                    .value = Some(*value);
                Ok(())
            }
            ApiAction::DropNestedCollection(slot) => {
                if let Some(collection) = &mut self.collections[*slot] {
                    collection.child = None;
                }
                Ok(())
            }
            ApiAction::DropCollection(slot) => {
                let collection = &mut self.collections[*slot];
                if collection
                    .as_ref()
                    .is_some_and(|collection| collection.child.is_some())
                {
                    Err(ApiTransitionError::NotEmpty)
                } else {
                    *collection = None;
                    Ok(())
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
    pub(super) fn new(nclients: usize) -> Self {
        let initial = BTreeSet::from([ApiModel::new()]);
        ApiAcct {
            possible: vec![initial; nclients],
        }
    }

    pub(super) fn possible(&self, client: usize) -> &BTreeSet<ApiModel> {
        &self.possible[client]
    }

    pub(super) fn project(
        before: &BTreeSet<ApiModel>,
        program: &ApiTransaction,
    ) -> BTreeSet<ApiModel> {
        before
            .iter()
            .map(|model| Self::apply(model, program))
            .collect()
    }

    pub(super) fn begin(
        &mut self,
        program: &ApiTransaction,
    ) -> (BTreeSet<ApiModel>, BTreeSet<ApiModel>) {
        let before = self.possible[program.client].clone();
        let after = Self::project(&before, program);
        self.possible[program.client].extend(after.iter().cloned());
        (before, after)
    }

    pub(super) fn confirm(&mut self, client: usize, after: BTreeSet<ApiModel>) {
        self.possible[client] = after;
    }

    pub(super) fn contains(&self, client: usize, model: &ApiModel) -> bool {
        self.possible[client].contains(model)
    }

    fn apply(model: &ApiModel, program: &ApiTransaction) -> ApiModel {
        let mut next = model.clone();
        for action in &program.actions {
            // Expected API errors are accepted by the executor, so they do not
            // stop the rest of a modeled transaction program.
            let _ = next.apply(action);
        }
        next
    }
}

pub(super) fn possible_values(models: &BTreeSet<ApiModel>, key: usize) -> BTreeSet<Option<u8>> {
    models.iter().map(|model| model.values[key]).collect()
}

pub(super) fn expected_collection_states(
    models: &BTreeSet<ApiModel>,
    slot: usize,
) -> BTreeSet<Option<ApiCollectionModel>> {
    models
        .iter()
        .map(|model| model.collections[slot].clone())
        .collect()
}

pub(super) fn expected_catalog_states(
    models: &BTreeSet<ApiModel>,
) -> BTreeSet<Vec<Option<ApiCollectionModel>>> {
    models
        .iter()
        .map(|model| model.collections.clone())
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiTransitionError {
    AlreadyExists,
    NotEmpty,
}

#[cfg(test)]
mod sim_tests {
    use super::*;

    type CollectionState = (usize, Option<u8>, Option<Option<u8>>);

    fn model(values: &[(usize, u8)], collections: &[CollectionState]) -> ApiModel {
        let mut model = ApiModel::new();
        for (key, value) in values {
            model.values[*key] = Some(*value);
        }
        for (slot, value, child) in collections {
            model.collections[*slot] = Some(ApiCollectionModel {
                value: *value,
                child: child.map(|value| ApiChildModel { value }),
            });
        }
        model
    }

    #[test]
    fn transition_vectors_preserve_states_and_errors() {
        use ApiAction::{
            CreateCollection, CreateCollectionIfAbsent, CreateNestedCollection, Delete,
            DropCollection, DropNestedCollection, InspectCollections, Read, ReadCollection, Write,
            WriteCollection, WriteNestedCollection,
        };
        use ApiTransitionError::{AlreadyExists, NotEmpty};

        let vectors = vec![
            (Read(0), Ok(()), model(&[], &[])),
            (Write(0, 7), Ok(()), model(&[(0, 7)], &[])),
            (Read(0), Ok(()), model(&[(0, 7)], &[])),
            (Delete(0), Ok(()), model(&[], &[])),
            (ReadCollection(0), Ok(()), model(&[], &[])),
            (DropNestedCollection(0), Ok(()), model(&[], &[])),
            (DropCollection(0), Ok(()), model(&[], &[])),
            (CreateCollection(0), Ok(()), model(&[], &[(0, None, None)])),
            (
                CreateCollectionIfAbsent(0),
                Ok(()),
                model(&[], &[(0, None, None)]),
            ),
            (
                CreateCollection(0),
                Err(AlreadyExists),
                model(&[], &[(0, None, None)]),
            ),
            (
                WriteCollection(0, 11),
                Ok(()),
                model(&[], &[(0, Some(11), None)]),
            ),
            (
                ReadCollection(0),
                Ok(()),
                model(&[], &[(0, Some(11), None)]),
            ),
            (
                CreateNestedCollection(0),
                Ok(()),
                model(&[], &[(0, Some(11), Some(None))]),
            ),
            (
                CreateNestedCollection(0),
                Err(AlreadyExists),
                model(&[], &[(0, Some(11), Some(None))]),
            ),
            (
                DropCollection(0),
                Err(NotEmpty),
                model(&[], &[(0, Some(11), Some(None))]),
            ),
            (
                WriteNestedCollection(0, 12),
                Ok(()),
                model(&[], &[(0, Some(11), Some(Some(12)))]),
            ),
            (
                InspectCollections,
                Ok(()),
                model(&[], &[(0, Some(11), Some(Some(12)))]),
            ),
            (
                DropNestedCollection(0),
                Ok(()),
                model(&[], &[(0, Some(11), None)]),
            ),
            (DropCollection(0), Ok(()), model(&[], &[])),
            (
                CreateCollectionIfAbsent(0),
                Ok(()),
                model(&[], &[(0, None, None)]),
            ),
            (DropCollection(0), Ok(()), model(&[], &[])),
            (
                WriteCollection(1, 13),
                Ok(()),
                model(&[], &[(1, Some(13), None)]),
            ),
            (DropCollection(1), Ok(()), model(&[], &[])),
            (
                CreateNestedCollection(1),
                Ok(()),
                model(&[], &[(1, None, Some(None))]),
            ),
            (
                WriteNestedCollection(1, 14),
                Ok(()),
                model(&[], &[(1, None, Some(Some(14)))]),
            ),
            (
                DropNestedCollection(1),
                Ok(()),
                model(&[], &[(1, None, None)]),
            ),
            (DropCollection(1), Ok(()), model(&[], &[])),
            (
                WriteNestedCollection(1, 15),
                Ok(()),
                model(&[], &[(1, None, Some(Some(15)))]),
            ),
        ];

        let mut actual = ApiModel::new();
        for (index, (action, expected_error, expected_state)) in vectors.into_iter().enumerate() {
            assert_eq!(
                actual.apply(&action),
                expected_error,
                "transition vector {index} returned a different error for {action:?}"
            );
            assert_eq!(
                actual, expected_state,
                "transition vector {index} produced a different state for {action:?}"
            );
        }
    }

    #[test]
    fn reachable_state_transitions_preserve_in_doubt_outcomes() {
        let write = ApiTransaction {
            client: 0,
            actions: vec![ApiAction::Write(0, 7)],
            abort: false,
        };
        let delete = ApiTransaction {
            client: 0,
            actions: vec![ApiAction::Delete(0)],
            abort: false,
        };
        let aborting_write = ApiTransaction {
            client: 0,
            actions: vec![ApiAction::Write(0, 7)],
            abort: true,
        };
        let mut account = ApiAcct::new(2);

        let initial = BTreeSet::from([model(&[], &[])]);
        let written = BTreeSet::from([model(&[(0, 7)], &[])]);
        let (before_write, after_write) = account.begin(&write);
        assert_eq!(before_write, initial);
        assert_eq!(after_write, written);
        assert_eq!(
            account.possible(0),
            &BTreeSet::from([model(&[], &[]), model(&[(0, 7)], &[])])
        );
        assert_eq!(account.possible(1), &initial);

        account.confirm(0, after_write);
        assert_eq!(account.possible(0), &written);

        let (_, after_delete) = account.begin(&delete);
        assert_eq!(after_delete, initial);
        assert_eq!(
            account.possible(0),
            &BTreeSet::from([model(&[], &[]), model(&[(0, 7)], &[])])
        );
        account.confirm(0, after_delete);
        assert_eq!(account.possible(0), &initial);

        let aborted_projection = ApiAcct::project(account.possible(0), &aborting_write);
        assert_eq!(aborted_projection, written);
        assert_eq!(account.possible(0), &initial);
    }
}
