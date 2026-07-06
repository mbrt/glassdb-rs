# Coding Agents Instructions

## Design

- Keep ADRs minimal and focused on the decisions, architecture, and trade-offs. Avoid implementation details.
- When new ADRs make older ones obsolete, mark them as such and link to the new ADR.
- Add ADRs only for significant decisions.
- ADRs are frozen, except for their status and links to newer ADRs.

## Development

- No need to update PORTING.md anymore. This project is now independent of the original Go project
- Keep implementation methods in order of visibility
- Comments should be used to explain why something is done, not what
- Functions' docstring should describe the function's purpose, not its implementation, nor callers

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
