# Coding Agents Instructions

## Language

- Talk in ASD-STE100 Simplified Technical English. Avoid mannered prose
- In your language, comments, and documentation, use the terms defined in `GLOSSARY.md`

## Design

- Always respect the design principles in `docs/principles.md`
- Use the `glassdb-codebase-design` skill for designing functionality or refactoring.
- Use and keep [docs/architecture.md](./docs/architecture.md) up to date, but keep it minimal. Only add information that cannot be easily deduced from code (decisions, high-level algorithm, diagrams).
- ADRs are frozen when accepted, except for their status and links to newer ADRs.
- No need to design migrations yet, as the project is still unreleased.

## Development

- Follow the standards in [docs/guides/change-standards.md](docs/guides/change-standards.md) when you write a change
- Use `make format` to auto-format the code
- Use `make test` to run format checks, linting, and ordinary and simulation tests. Also run `make test-bench` for changes to benchmarks or CI comparison tools

## Verification

After you finish a sizable chunk of work, review it:

- Do not review work that is still exploratory (an investigation, an experiment, or a design that is not agreed yet). Every chunk that you commit gets a review
- Start a fresh sub-agent, not a fork, so that this conversation does not bias the review. Tell it to use the `glassdb-review` skill
- Give it the inputs listed in the skill: the target (the diff since the last review, or the branch against its merge-base with `main`), the goal, and the decisions agreed with the user. Do not give it your arguments for why the change is correct, or a summary of the conversation
- Handle its findings with the `glassdb-triage` skill
