---
name: glassdb-triage
description: Decide which GlassDB review findings to fix, ask about, or drop, and fix them at the cause. Use when you act on review findings, after a review that AGENTS.md asks for, or when the user asks you to fix findings.
---

# Handle review findings

A finding is a claim to check, not an order. A fix that you apply without
judgment often adds more complexity than the problem it solves.

## Sort each finding

Check the finding against the code first. Then put it in one group:

- **Fix:** the finding is true, it has a failure scenario, and the fix stays
  inside the agreed scope.
- **Ask the user:** the finding is true, but the fix changes something that the
  user decides. Examples:
  - It conflicts with an agreed decision.
  - It changes the scope of the change.
  - It changes what a test or a benchmark measures, a default, or the policy
    that a test uses.
  - The finding is a question about a risk with no known failure mechanism.
- **Drop:** the finding is false, or the fix that it asks for breaks a
  standard. Examples: compatibility shims, visibility widened for tests, or a
  guard for a risk with no known failure.

If there are "ask the user" findings, stop before you apply any fix. Ask all
the questions in one message. Give each question the finding, the evidence,
and the options. Then apply the fixes and the answers together.

## Fix the cause

- Prefer a fix that removes the cause to a fix that hides the symptom. For
  example, if warm-up work leaks into a measurement, wait for the warm-up to
  end. Do not only stop counting it.
- Prefer the smallest fix. If a fix adds a parameter, a callback, a gate, a
  trait method, or a new public method, look for a fix without it. If there is
  none, compare its cost with the cost of the problem. The answer can be to
  drop the finding.
- Apply the [change standards](../../../docs/guides/change-standards.md) to
  the fix. A fix is code too.

## Report

Report to the user:

- The findings that you fixed, with one line each.
- The findings that you dropped, with the reason.
- The questions for the user.

Do not commit or open a PR unless the user asks for it.
