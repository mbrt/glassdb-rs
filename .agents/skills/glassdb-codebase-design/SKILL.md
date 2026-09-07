---
name: glassdb-codebase-design
description: Design or improve GlassDB interfaces, responsibility seams, and testability. Use for high level architectural decisions, module design, and refactoring.
---

# Codebase design

Two levels of design:

1. Mechanism, where behavior is defined or changed, and the cost of changing it is high (e.g. requires a migration)
2. Architectural, where the shape of a new (sub-)system is defined, or components responsibilities must change

Increasing level of care is necessary when:

- Fixing a bug without changing the interface, nor on-the-wire format
- Adding or removing functionality to a private module
- Expanding the responsibilities of an established component
- Changing a previously settled trade-off
- Adding or removing functionality to the public API (e.g. `glassdb` crate)
- Changing user-visible behavior or the on-the-wire format

Not all changes require both levels of design, and judgment is necessary to determine which level is appropriate for the change in hand. When solving a problem, generally prefer the lowest impact that solves it.

If one or both levels are required, frontload discussion and ADR writing before implementing.

## ADRs

Only offer to create an ADR when all three are true:

1. **Hard to reverse**: the cost of changing your mind later is meaningful
2. **Surprising without context**: a future reader will wonder "why did they do it this way?"
3. **The result of a real trade-off**: there were genuine alternatives and you picked one for specific reasons

If any of the three is missing, skip the ADR.

See [adr-design.md](./adr-design.md) for guidance on writing ADRs.

## Structure

Before updating code, consider where functionality should live and which component(s) should own it.

See [interface-design.md](./interface-design.md) for guidance on well-structured interfaces and ways to improve them.

## Refactor

Identify refactoring opportunities proactively, when:

- Interfaces expand beyond their original intent
- Behavior leaks across modules
- There are deepening opportunities
- Strive to reduce complexity and number of lines of code (excluding tests)

See [interface-design.md](./interface-design.md) for guidance on designing interfaces and seams.
