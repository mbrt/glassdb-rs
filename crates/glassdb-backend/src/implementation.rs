//! Support for implementing and testing [`Backend`].
//!
//! Applications using an existing backend normally do not need this module.
//! Backend implementations use these helpers to preserve the shared listing
//! cursor contract without interpreting another provider's token.

use std::collections::HashSet;

use crate::{Backend, BackendError, ListCursor, ListLimit};

const CONFORMANCE_PREFIX: &str = "__glassdb_list_conformance__/target/";
const CONFORMANCE_EMPTY_PREFIX: &str = "__glassdb_list_conformance__/empty/";
const CONFORMANCE_INVALID_PREFIX: &str = "__glassdb_list_conformance__/invalid";
const CONFORMANCE_MAX_PAGES: usize = 16;

const CONFORMANCE_EXPECTED: [&str; 5] = [
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/middle",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/target/zulu",
];

// The write order intentionally differs from lexical order and interleaves
// recursive matches with keys beside, and merely near, the requested prefix.
const CONFORMANCE_SEED_KEYS: [&str; 7] = [
    "__glassdb_list_conformance__/target/nested/deeper/charlie",
    "__glassdb_list_conformance__/sibling/ignored",
    "__glassdb_list_conformance__/target/zulu",
    "__glassdb_list_conformance__/targetish/ignored",
    "__glassdb_list_conformance__/target/alpha",
    "__glassdb_list_conformance__/target/nested/bravo",
    "__glassdb_list_conformance__/target/middle",
];

/// Binds a nonempty provider continuation token to its listing prefix.
pub fn bind_list_cursor(prefix: &str, provider_token: &str) -> Result<ListCursor, BackendError> {
    validate_list_prefix(prefix)?;
    if provider_token.is_empty() {
        return Err(BackendError::other(
            "list provider returned an empty continuation token",
        ));
    }
    Ok(ListCursor {
        prefix: prefix.into(),
        provider_token: provider_token.into(),
    })
}

/// Validates a listing prefix and cursor and returns the provider token.
pub fn list_provider_token<'a>(
    prefix: &str,
    cursor: Option<&'a ListCursor>,
) -> Result<Option<&'a str>, BackendError> {
    validate_list_prefix(prefix)?;
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    if cursor.prefix.as_ref() != prefix {
        return Err(BackendError::InvalidCursor);
    }
    Ok(Some(cursor.provider_token.as_ref()))
}

/// Asserts the provider-independent recursive-listing contract.
///
/// Run this against a fresh, disposable backend. It writes fixtures under the
/// reserved `__glassdb_list_conformance__/` prefix and leaves them in place.
pub async fn assert_list_conformance(backend: &dyn Backend) {
    for path in CONFORMANCE_SEED_KEYS {
        backend
            .write_if_not_exists(path, path.as_bytes().to_vec())
            .await
            .unwrap_or_else(|error| panic!("failed to seed {path:?}: {error:?}"));
    }

    let expected: HashSet<String> = CONFORMANCE_EXPECTED
        .into_iter()
        .map(str::to_owned)
        .collect();
    let limit = ListLimit::new(2).unwrap();
    let mut objects = HashSet::new();
    let mut cursors = HashSet::new();
    let mut cursor = None;
    let mut first_cursor = None;
    let mut pages = 0;
    let mut terminated = false;

    for _ in 0..CONFORMANCE_MAX_PAGES {
        pages += 1;
        let page = backend
            .list(CONFORMANCE_PREFIX, cursor.as_ref(), limit)
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
            "listing repeated cursor {next:?}"
        );
        first_cursor.get_or_insert_with(|| next.clone());
        cursor = Some(next);
    }

    assert!(
        terminated,
        "listing did not terminate within {CONFORMANCE_MAX_PAGES} pages"
    );
    assert!(pages > 1, "fixture did not exercise pagination");
    assert_eq!(objects, expected, "recursive listing membership differed");

    let first_cursor = first_cursor.expect("paginated fixture returned no cursor");
    let error = backend
        .list(CONFORMANCE_EMPTY_PREFIX, Some(&first_cursor), limit)
        .await
        .unwrap_err();
    assert!(
        matches!(error, BackendError::InvalidCursor),
        "cursor reused under another prefix returned {error:?}"
    );

    let empty_page = backend
        .list(CONFORMANCE_EMPTY_PREFIX, None, limit)
        .await
        .unwrap();
    assert!(
        empty_page.objects.is_empty(),
        "empty prefix returned objects"
    );
    assert!(empty_page.next.is_none(), "empty prefix returned a cursor");

    let invalid_provider_cursor = bind_list_cursor(CONFORMANCE_PREFIX, "invalid").unwrap();
    for cursor in [None, Some(&first_cursor)] {
        let error = backend
            .list(CONFORMANCE_INVALID_PREFIX, cursor, limit)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, BackendError::Other { .. }),
            "invalid prefix returned {error:?}"
        );
    }
    let error = backend
        .list(CONFORMANCE_PREFIX, Some(&invalid_provider_cursor), limit)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, BackendError::InvalidCursor),
        "provider-invalid cursor returned {error:?}"
    );
}

fn validate_list_prefix(prefix: &str) -> Result<(), BackendError> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Ok(())
    } else {
        Err(BackendError::other(format!(
            "list prefix must be empty or end in '/': {prefix:?}"
        )))
    }
}
