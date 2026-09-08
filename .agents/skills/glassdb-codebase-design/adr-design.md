# ADR design

Record only meaningful architectural decisions and explicit trade-offs in ADRs (`docs/adr/*.md`), not implementation details.

## Discussion

Make sure important decisions and trade-offs are first discussed with the user. Use the `glassdb-grill` skill to discuss them.

Questions should revolve around:

- Uncovering whether your understanding is aligned with the user's intent
- Major trade-offs between alternative designs
- Should stay high-level

## Writing

After settling decisions, proceed with writing the ADR. When writing a new ADR:

- Use the format in `docs/adr/000-template.md`
- Keep them minimal and focused on decisions, architecture, and trade-offs
- Avoid implementation details
- Keep the writing concise but readable. Don't use complex prose or unexplained terms
- Use language from `CONTEXT.md`
- If in need for a new term, use the `glassdb-domain-modeling` skill to define it in `CONTEXT.md`

## Updating

- When new ADRs make older ones obsolete, mark them as such and link to the new ADR
- ADRs are frozen when accepted, except for their status and links to newer ADRs
- Update [docs/architecture.md](./docs/architecture.md) to keep it up to date with accepted ADRs, but only after the implementation lands
