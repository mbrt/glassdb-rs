//! Behavioral assertions for the shared object-storage contract.

use std::collections::HashSet;
use std::fmt::Debug;
use std::num::NonZeroUsize;

use glassdb_concurr::join_all_bounded;

use crate::{Backend, BackendError, ListCursor, ListLimit, ReadReply, Version};

const LIST_PREFIX: &str = "__glassdb_list_conformance__/target/";
const EMPTY_PREFIX: &str = "__glassdb_list_conformance__/empty/";
const INVALID_PREFIX: &str = "__glassdb_list_conformance__/invalid";

const LIST_EXPECTED: [&str; 5] = [
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/middle",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/target/zulu",
];

// A different write order and near-prefix paths expose ordering and filtering
// mistakes without requiring a particular provider's result order.
const LIST_SEED_PATHS: [&str; 7] = [
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/sibling/ignored",
    "__glassdb_list_conformance__/target/zulu",
    "__glassdb_list_conformance__/targetish/ignored",
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/middle",
];

/// Asserts the object-storage behavior required by GlassDB.
///
/// Run this against a fresh, empty, disposable backend, without external
/// writers or injected faults. It leaves fixtures under
/// `__glassdb_backend_conformance__/` and `__glassdb_list_conformance__/`.
/// Transport fault classification requires separate provider-specific tests.
///
/// # Panics
///
/// Panics with the operation and fixture path when a contract check fails.
pub async fn assert_backend_conformance(backend: &dyn Backend) {
    assert_list_conformance(backend).await;
    for (case, contents) in [
        ("text", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("nested/binary", (0..=255).collect()),
    ] {
        let path = format!("__glassdb_backend_conformance__/contents/{case}");
        assert_object_lifecycle(backend, &path, contents).await;
    }
    assert_competing_writes(backend, true).await;
    assert_competing_writes(backend, false).await;
    assert_write_delete_contest(backend).await;
}

/// Asserts the provider-independent recursive-listing contract.
///
/// Run this against a fresh, empty, disposable backend. It writes fixtures
/// under `__glassdb_list_conformance__/` and leaves them in place. No other
/// writer may change the backend during the assertions.
pub async fn assert_list_conformance(backend: &dyn Backend) {
    let limit = ListLimit::new(2).unwrap();
    assert_listing(backend, "", limit, &[]).await;
    for path in LIST_SEED_PATHS {
        success(
            path,
            "list fixture create",
            backend
                .write_if_not_exists(path, path.as_bytes().to_vec())
                .await,
        );
    }

    let first_cursor = assert_listing(backend, LIST_PREFIX, limit, &LIST_EXPECTED)
        .await
        .expect("list target: fixtures must exercise pagination");
    for limit in [ListLimit::MIN, ListLimit::new(10).unwrap()] {
        assert_listing(backend, LIST_PREFIX, limit, &LIST_EXPECTED).await;
    }
    assert_listing(backend, "", limit, &LIST_SEED_PATHS).await;
    assert_listing(backend, EMPTY_PREFIX, limit, &[]).await;
    assert_listing(
        backend,
        "__glassdb_list_conformance__/target/nested/",
        limit,
        &LIST_EXPECTED[2..4],
    )
    .await;

    let result = backend.list(EMPTY_PREFIX, Some(&first_cursor), limit).await;
    assert!(
        matches!(result, Err(BackendError::InvalidCursor)),
        "list {EMPTY_PREFIX:?}: cursor from another prefix returned {result:?}"
    );
    for cursor in [None, Some(&first_cursor)] {
        let result = backend.list(INVALID_PREFIX, cursor, limit).await;
        assert!(
            matches!(result, Err(BackendError::Other { .. })),
            "list {INVALID_PREFIX:?}: invalid prefix returned {result:?}"
        );
    }
}

async fn assert_object_lifecycle(backend: &dyn Backend, path: &str, contents: Vec<u8>) {
    let unset = Version::default();
    assert_absent(backend, path, &unset).await;
    let result = backend.write_if(path, b"unset".to_vec(), &unset).await;
    assert!(
        matches!(
            result,
            Err(BackendError::NotFound | BackendError::Precondition)
        ),
        "unset-version write to absent {path:?}: expected NotFound or Precondition, got {result:?}"
    );
    assert_absent(backend, path, &unset).await;
    let initial = success(
        path,
        "create",
        backend.write_if_not_exists(path, contents.clone()).await,
    );
    assert_object(backend, path, &contents, &initial).await;
    assert_precondition(
        path,
        "unchanged read",
        backend.read_if_modified(path, &initial).await,
    );
    assert_reply(
        path,
        "unset-version read",
        success(
            path,
            "unset-version read",
            backend.read_if_modified(path, &unset).await,
        ),
        &contents,
        &initial,
    );

    for duplicate in [contents.clone(), b"duplicate".to_vec()] {
        assert_precondition(
            path,
            "duplicate create",
            backend.write_if_not_exists(path, duplicate).await,
        );
        assert_object(backend, path, &contents, &initial).await;
    }
    assert_precondition(
        path,
        "unset-version write",
        backend.write_if(path, b"unset".to_vec(), &unset).await,
    );
    assert_object(backend, path, &contents, &initial).await;
    assert_precondition(
        path,
        "unset-version delete",
        backend.delete_if(path, &unset).await,
    );
    assert_object(backend, path, &contents, &initial).await;

    // Equivalent content may keep its token, as it does with content-based
    // versions. Only a change in content must invalidate the previous token.
    let same = success(
        path,
        "same-content write",
        backend.write_if(path, contents.clone(), &initial).await,
    );
    assert_object(backend, path, &contents, &same).await;
    let changed_contents = b"changed contents";
    let changed = success(
        path,
        "changed-content write",
        backend
            .write_if(path, changed_contents.to_vec(), &same)
            .await,
    );
    assert_ne!(
        same, changed,
        "write {path:?}: different contents retained the version"
    );
    assert_object(backend, path, changed_contents, &changed).await;
    assert_reply(
        path,
        "modified read",
        success(
            path,
            "modified read",
            backend.read_if_modified(path, &same).await,
        ),
        changed_contents,
        &changed,
    );
    assert_precondition(
        path,
        "unchanged read after write",
        backend.read_if_modified(path, &changed).await,
    );
    assert_precondition(
        path,
        "stale write",
        backend.write_if(path, b"stale".to_vec(), &same).await,
    );
    assert_object(backend, path, changed_contents, &changed).await;
    assert_precondition(path, "stale delete", backend.delete_if(path, &same).await);
    assert_object(backend, path, changed_contents, &changed).await;

    success(
        path,
        "matching delete",
        backend.delete_if(path, &changed).await,
    );
    assert_absent(backend, path, &changed).await;
    let result = backend.delete_if(path, &changed).await;
    assert!(
        matches!(result, Ok(()) | Err(BackendError::NotFound)),
        "missing delete {path:?}: expected success or NotFound, got {result:?}"
    );
    assert_absent(backend, path, &changed).await;
    let result = backend.write_if(path, b"missing".to_vec(), &changed).await;
    assert!(
        matches!(
            result,
            Err(BackendError::NotFound | BackendError::Precondition)
        ),
        "missing write {path:?}: expected NotFound or Precondition, got {result:?}"
    );
    assert_absent(backend, path, &changed).await;

    let recreated = success(
        path,
        "recreate",
        backend.write_if_not_exists(path, contents.clone()).await,
    );
    assert_ne!(
        changed, recreated,
        "recreate {path:?}: different contents retained the version"
    );
    assert_object(backend, path, &contents, &recreated).await;
    assert_precondition(
        path,
        "delete with old version after recreation",
        backend.delete_if(path, &changed).await,
    );
    assert_object(backend, path, &contents, &recreated).await;
    assert_precondition(
        path,
        "write with old version after recreation",
        backend.write_if(path, b"stale".to_vec(), &changed).await,
    );
    assert_object(backend, path, &contents, &recreated).await;
    assert_reply(
        path,
        "read after recreation",
        success(
            path,
            "read after recreation",
            backend.read_if_modified(path, &changed).await,
        ),
        &contents,
        &recreated,
    );
}

async fn assert_competing_writes(backend: &dyn Backend, create: bool) {
    let path = if create {
        "__glassdb_backend_conformance__/competing/create"
    } else {
        "__glassdb_backend_conformance__/competing/write"
    };
    let expected = if create {
        None
    } else {
        Some(success(
            path,
            "seed CAS contest",
            backend.write_if_not_exists(path, b"initial".to_vec()).await,
        ))
    };
    let values = [b"left".as_slice(), b"right".as_slice()];
    let outcomes = join_all_bounded(
        values.iter().map(|contents| async {
            match &expected {
                None => backend.write_if_not_exists(path, contents.to_vec()).await,
                Some(version) => backend.write_if(path, contents.to_vec(), version).await,
            }
        }),
        NonZeroUsize::new(2).unwrap(),
    )
    .await;
    let mut winner = None;
    for (contents, outcome) in values.into_iter().zip(outcomes) {
        match outcome {
            Ok(version) => {
                assert!(
                    winner.is_none(),
                    "competing writes {path:?}: both mutations succeeded"
                );
                winner = Some((contents, version));
            }
            Err(BackendError::Precondition) => {}
            error => {
                panic!("competing writes {path:?}: expected success or Precondition, got {error:?}")
            }
        }
    }
    let (contents, version) =
        winner.unwrap_or_else(|| panic!("competing writes {path:?}: neither mutation succeeded"));
    assert_object(backend, path, contents, &version).await;
}

async fn assert_write_delete_contest(backend: &dyn Backend) {
    let path = "__glassdb_backend_conformance__/competing/write-delete";
    let initial = success(
        path,
        "seed write/delete contest",
        backend.write_if_not_exists(path, b"initial".to_vec()).await,
    );
    let expected = &initial;
    let contents = b"changed contents";
    let outcomes = join_all_bounded(
        [false, true].map(|delete| async move {
            if delete {
                backend.delete_if(path, expected).await.map(|()| None)
            } else {
                backend
                    .write_if(path, contents.to_vec(), expected)
                    .await
                    .map(Some)
            }
        }),
        NonZeroUsize::new(2).unwrap(),
    )
    .await;
    match outcomes.as_slice() {
        [Ok(Some(version)), Err(BackendError::Precondition)] => {
            assert_object(backend, path, contents, version).await;
        }
        [
            Err(BackendError::NotFound | BackendError::Precondition),
            Ok(None),
        ] => {
            assert_absent(backend, path, expected).await;
        }
        outcomes => panic!("write/delete contest {path:?}: invalid outcomes {outcomes:?}"),
    }
}

async fn assert_listing(
    backend: &dyn Backend,
    prefix: &str,
    limit: ListLimit,
    expected: &[&str],
) -> Option<ListCursor> {
    let expected: HashSet<String> = expected.iter().map(|path| (*path).to_string()).collect();
    let mut objects = HashSet::new();
    let mut cursor = None;
    let mut first_cursor = None;
    let mut page_number = 0_usize;
    loop {
        page_number += 1;
        let page = success(
            prefix,
            "list",
            backend.list(prefix, cursor.as_ref(), limit).await,
        );
        assert!(
            page.objects.len() <= limit.get(),
            "list {prefix:?}, page {page_number}: exceeded limit {limit}: {:?}",
            page.objects
        );
        for path in page.objects {
            assert!(
                expected.contains(&path),
                "list {prefix:?}, page {page_number}: unexpected path {path:?}"
            );
            assert!(
                objects.insert(path.clone()),
                "list {prefix:?}, page {page_number}: duplicate path {path:?}"
            );
        }
        let Some(next) = page.next else {
            assert_eq!(objects, expected, "list {prefix:?}: membership differed");
            return first_cursor;
        };
        first_cursor.get_or_insert_with(|| next.clone());
        cursor = Some(next);
    }
}

async fn assert_absent(backend: &dyn Backend, path: &str, expected: &Version) {
    let result = backend.read(path).await;
    assert!(
        matches!(result, Err(BackendError::NotFound)),
        "read absent {path:?}: expected NotFound, got {result:?}"
    );
    let result = backend.read_if_modified(path, expected).await;
    assert!(
        matches!(result, Err(BackendError::NotFound)),
        "conditional read absent {path:?}: expected NotFound, got {result:?}"
    );
}

async fn assert_object(backend: &dyn Backend, path: &str, contents: &[u8], version: &Version) {
    let reply = success(path, "read", backend.read(path).await);
    assert_reply(path, "read", reply, contents, version);
}

#[track_caller]
fn assert_reply(path: &str, operation: &str, reply: ReadReply, contents: &[u8], version: &Version) {
    assert_eq!(
        reply.contents, contents,
        "{operation} {path:?}: contents differed"
    );
    assert!(
        !reply.version.is_unset(),
        "{operation} {path:?}: version is unset"
    );
    assert_eq!(
        &reply.version, version,
        "{operation} {path:?}: version differed"
    );
}

#[track_caller]
fn success<T>(path: &str, operation: &str, result: Result<T, BackendError>) -> T {
    result.unwrap_or_else(|error| panic!("{operation} {path:?}: expected success, got {error:?}"))
}

#[track_caller]
fn assert_precondition<T: Debug>(path: &str, operation: &str, result: Result<T, BackendError>) {
    assert!(
        matches!(result, Err(BackendError::Precondition)),
        "{operation} {path:?}: expected Precondition, got {result:?}"
    );
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::ListPage;
    use crate::implementation::{bind_list_cursor, list_provider_token};
    use crate::memory::MemoryBackend;

    struct EmptyAndShortPages(MemoryBackend);

    #[async_trait]
    impl Backend for EmptyAndShortPages {
        async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
            self.0.read(path).await
        }

        async fn read_if_modified(
            &self,
            path: &str,
            expected: &Version,
        ) -> Result<ReadReply, BackendError> {
            self.0.read_if_modified(path, expected).await
        }

        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BackendError> {
            self.0.write_if(path, value, expected).await
        }

        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<Version, BackendError> {
            self.0.write_if_not_exists(path, value).await
        }

        async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
            self.0.delete_if(path, expected).await
        }

        async fn list(
            &self,
            prefix: &str,
            cursor: Option<&ListCursor>,
            _limit: ListLimit,
        ) -> Result<ListPage, BackendError> {
            let token = list_provider_token(prefix, cursor)?;
            let empty_page = match token {
                None => 0,
                Some(token) => match token.strip_prefix("empty:") {
                    Some(page) => page.parse::<usize>().unwrap(),
                    None => return self.0.list(prefix, cursor, ListLimit::MIN).await,
                },
            };
            // Exceed the former guard of 64 pages, including for empty prefixes.
            if empty_page < 65 {
                return Ok(ListPage {
                    objects: Vec::new(),
                    next: Some(bind_list_cursor(
                        prefix,
                        &format!("empty:{}", empty_page + 1),
                    )?),
                });
            }
            self.0.list(prefix, None, ListLimit::MIN).await
        }
    }

    #[tokio::test]
    async fn conformance_accepts_empty_and_short_listing_pages() {
        assert_list_conformance(&EmptyAndShortPages(MemoryBackend::new())).await;
    }
}
