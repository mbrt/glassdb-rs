---
name: glassdb-domain-modeling
description: Sharpen GlassDB terminology. Use when discussing codebase terminology, editing CONTEXT.md, ADRs, or architecture documentation.
---

# Domain modeling

Actively build and sharpen the project's domain model as you design. This is the *active* discipline: challenging terms, inventing edge-case scenarios, and writing the glossary and decisions down the moment they crystallise. This skill is for when you're changing the model, not just consuming it.

## Structure

Use `CONTEXT.md` at the root of the repository as the canonical GlassDB glossary. Before changing it or recording a decision, read the relevant code, tests, `docs/architecture.md`, `docs/principles.md`, and accepted ADRs.

## Language

## Challenge against the glossary

When the user uses a term that conflicts with the existing language in `CONTEXT.md`, call it out immediately. "Your glossary defines 'cancellation' as X, but you seem to mean Y. Which is it?"

### Sharpen fuzzy language

When the user uses vague or overloaded terms, propose a precise canonical term. "You're saying 'account': do you mean the Customer or the User? Those are different things."

### Discuss concrete scenarios

When domain relationships are being discussed, stress-test them with specific scenarios. Invent scenarios that probe edge cases and force the user to be precise about the boundaries between concepts.

### Cross-reference with code

When the user states how something works, check whether the code agrees. If you find a contradiction, surface it: "Your code cancels entire Orders, but you just said partial cancellation is possible. Which is right?"

When the code terminology is inconsistent within itself or with the glossary, call it out with the goal of achieving a single, consistent model.

### Update CONTEXT.md inline

When a term is resolved, update `CONTEXT.md` right there. Don't batch these up: capture them as they happen. Use the format in [context-format.md](./context-format.md).

`CONTEXT.md` should be totally devoid of implementation details. Do not treat `CONTEXT.md` as a spec, a scratch pad, or a repository for implementation decisions. It is a glossary and nothing else.
