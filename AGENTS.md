# Coding Agents Instructions

## Development

### Tests

Always use `make test` to run tests. This runs format checks, linting
(`clippy -D warnings`), and the test suite:

```bash
make test
```

- Test interfaces and intended behavior instead of internals
- Prefer integration tests to mocks as much as possible

### Formatting Code

Use `make format` to auto-format the code:

```bash
make format
```

### Before Committing

Run `make test` to ensure all checks pass before committing changes.

### Dependencies

Add or update dependencies with `cargo`:

```bash
cargo add $crate
```
