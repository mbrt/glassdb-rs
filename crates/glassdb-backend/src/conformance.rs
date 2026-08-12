//! Shared behavioral checks for backend implementations.

use std::collections::HashSet;

use crate::{Backend, BackendError, ListCursor, ListLimit, bind_list_cursor};

const PREFIX: &str = "__glassdb_list_conformance__/target/";
const EMPTY_PREFIX: &str = "__glassdb_list_conformance__/empty/";
const INVALID_PREFIX: &str = "__glassdb_list_conformance__/invalid";
const MAX_PAGES: usize = 16;

const EXPECTED: [&str; 5] = [
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/middle",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/target/zulu",
];

// The write order intentionally differs from lexical order and interleaves
// recursive matches with keys beside, and merely near, the requested prefix.
const SEED_KEYS: [&str; 7] = [
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/sibling/ignored",
    "__glassdb_list_conformance__/target/zulu",
    "__glassdb_list_conformance__/targetish/ignored",
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/middle",
];

/// Asserts the provider-independent recursive-listing contract.
pub async fn assert_list_conformance(backend: &dyn Backend) {
    for path in SEED_KEYS {
        backend
            .write_if_not_exists(path, path.as_bytes().to_vec())
            .await
            .unwrap_or_else(|error| panic!("failed to seed {path:?}: {error:?}"));
    }

    let expected: HashSet<String> = EXPECTED.into_iter().map(str::to_owned).collect();
    let limit = ListLimit::new(2).unwrap();
    let mut objects = HashSet::new();
    let mut cursors = HashSet::new();
    let mut cursor = None;
    let mut first_cursor = None;
    let mut pages = 0;
    let mut terminated = false;

    for _ in 0..MAX_PAGES {
        pages += 1;
        let page = backend
            .list(PREFIX, cursor.as_ref(), limit)
            .await
            .unwrap_or_else(|error| panic!("list page {pages} failed: {error:?}"));
        assert!(
            page.objects.len() <= limit.get(),
            "page {pages} exceeded the requested limit: {:?}",
            page.objects
        );
        for path in page.objects {
            assert!(
                expected.contains(&path),
                "listing leaked a sibling or near-prefix key: {path:?}"
            );
            assert!(objects.insert(path.clone()), "duplicate object: {path:?}");
        }

        let Some(next) = page.next else {
            terminated = true;
            break;
        };
        assert!(
            cursors.insert(next.clone()),
            "listing repeated cursor {:?}",
            next.as_str()
        );
        first_cursor.get_or_insert_with(|| next.clone());
        cursor = Some(next);
    }

    assert!(
        terminated,
        "listing did not terminate within {MAX_PAGES} pages"
    );
    assert!(pages > 1, "fixture did not exercise pagination");
    assert_eq!(objects, expected, "recursive listing membership differed");

    let first_cursor = first_cursor.expect("paginated fixture returned no cursor");
    let error = backend
        .list(EMPTY_PREFIX, Some(&first_cursor), limit)
        .await
        .unwrap_err();
    assert!(
        matches!(error, BackendError::InvalidCursor),
        "cursor reused under another prefix returned {error:?}"
    );

    let empty_page = backend.list(EMPTY_PREFIX, None, limit).await.unwrap();
    assert!(
        empty_page.objects.is_empty(),
        "empty prefix returned objects"
    );
    assert!(empty_page.next.is_none(), "empty prefix returned a cursor");

    let invalid_cursor = ListCursor::new("invalid");
    let empty_cursor = ListCursor::new("");
    let invalid_provider_cursor = bind_list_cursor(PREFIX, "invalid").unwrap();
    for cursor in [None, Some(&invalid_cursor), Some(&empty_cursor)] {
        let error = backend
            .list(INVALID_PREFIX, cursor, limit)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, BackendError::Other { .. }),
            "invalid prefix returned {error:?}"
        );
    }
    for cursor in [&invalid_cursor, &empty_cursor, &invalid_provider_cursor] {
        let error = backend.list(PREFIX, Some(cursor), limit).await.unwrap_err();
        assert!(
            matches!(&error, BackendError::InvalidCursor),
            "invalid cursor returned {error:?}"
        );
    }
}
