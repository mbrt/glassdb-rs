# Coding Agents Instructions

## Language

- Talk in ASD-STE100 Simplified Technical English. Avoid mannered prose
- In your language, comments, and documentation, use the terms defined in `CONTEXT.md`

## Design

- Always respect the design principles in `docs/principles.md`
- Use the `glassdb-codebase-design` skill for designing functionality or refactoring.
- Use and keep [docs/architecture.md](./docs/architecture.md) up to date with the current architecture and design decisions.
- ADRs are frozen when accepted, except for their status and links to newer ADRs.

## Development

- Keep implementation methods in order of visibility
- Comments should be used to explain why something is done, not what
- When needing to explain what is done, consider whether the code can be refactored to make it self-explanatory instead
- When code has to be complex and hard to understand, it must be for a good reason, and the reason must be commented
- Functions' docstring should describe the function's purpose, not its implementation, nor callers
- Use `make format` to auto-format the code

### Tests

Always use `make test-all` to run tests. This runs format checks, linting, and the test suite

- Test interfaces and intended behavior instead of internals
- Avoid tautological tests
- Strongly prefer integration tests over mocks
- Always add deterministic regression tests when fixing bugs, they also serve as documentation
- Return errors instead of using assertions in transaction bodies, as they are not snapshot-transparent
