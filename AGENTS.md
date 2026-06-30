# Coding Agents Instructions

## Development

- No need to update PORTING.md anymore. This project is now independent of the original Go project.

### Tests

Always use `make test-all` to run tests. This runs format checks, linting
(`clippy -D warnings`), and the test suite.

- Test interfaces and intended behavior instead of internals
- Prefer integration tests to mocks as much as possible
- Always add deterministic regression tests when fixing bugs, they also serve as documentation

### Formatting Code

Use `make format` to auto-format the code:

```bash
make format
```
