# API migrations

## Backend listing requests

External `Backend` implementations must replace the removed raw listing method:

```rust,ignore
async fn list(
    &self,
    prefix: &str,
    cursor: Option<&ListCursor>,
    limit: ListLimit,
) -> Result<ListPage, BackendError>;
```

with the required request-taking contract:

```rust,ignore
async fn list_request(
    &self,
    request: ListRequest<'_>,
) -> Result<ListPage, BackendError>;
```

Use `request.prefix()`, `request.limit()`, and `request.provider_cursor()` when
building the provider request. `provider_cursor()` is already validated and
unwrapped; implementations should not decode `request.cursor()` themselves.
Callers construct a request with `ListRequest::new(prefix, cursor, limit)?` and
pass it to `list_request`. The raw `Backend::list`, `CachedStore::list`,
`validate_list_args`, and `validate_list_args_and_cursor` compatibility APIs
have been removed. `bind_list_cursor` remains available for implementations to
bind a nonempty provider continuation token to the request prefix.
