---
name: glassdb-align
description: Agree with the user on a GlassDB bug fix, design, or implementation before you build it. Use when the user asks you to reproduce or root-cause an issue and discuss fixes, to propose a solution "we discuss together", to design something together, or to stress-test a plan.
---

# Align with the user

The user decides the design. You bring the facts, one proposal, and only the
questions that the user must answer.

Facts come first, every time: get them before you ask a question and before
you answer one.

## 1. Get the facts first

Do not ask questions before you have facts. Use sub-agents where it helps.

- **Bug fix:** reproduce the bug. Find the root cause, not only the trigger.
  Have evidence, for example a failing test, a seed, or a log line.
- **Performance improvement:** before the design, get a benchmark or another
  way to show the inefficiency, and measure it. Propose a target value for
  the metric.
- **Design:** state the goal in your words and a check that shows success, for
  example a benchmark that shows the gap or a test that fails today.
- **Implementation:** read the code that the change touches. Find the
  mechanisms that you can reuse.

If a step takes long, for example a reproduction or a measurement, start the
discussion while it runs.

## 2. Send one proposal

Put these parts in one message, in this order. Leave out the parts that do not
apply.

1. **Goal** and the success check, in one or two lines. For a performance
   improvement, include the measured value and the target.
2. **Root cause** and its evidence (bug fix only).
3. **Design** that you recommend. Give each alternative that you rejected one
   line on why.
4. **Implementation shape:** the files, the new names, where the state lives,
   and the existing code that you reuse.
5. **Defaults:** the decisions that you took yourself, in a short list. The user
   can change any of them.
6. **Forks:** the decisions that the user must make. Three at most.

Before you send it, check the proposal against `docs/principles.md` and these
questions:

- Is there an existing mechanism that does this already?
- Does the fix remove the cause, or does it only hide a symptom?
- Can a value be computed instead of added as a setting?
- Is each new parameter, field, method, or piece of wiring necessary?
- Does an ADR fix only decisions that are hard to undo, and not constants?

## 3. Ask only real forks

Ask a question only if all of these are true:

- The answer changes what you build.
- The code, the docs, the principles, or a measurement cannot answer it.
- The decision is hard to undo or the user can see it, for example a public
  API, an ADR, or a public name.

Otherwise, decide and put the decision in the defaults. A change that grows the
scope is a default too: leave it out of this change and list it as a possible
follow-up.

Ask the forks with the `AskUserQuestion` tool, after the proposal text. Put your
recommended option first. If a question does not fit two to four options, for
example a question about the intent of a test, ask it in plain text.

Do not ask a question to confirm your preferred answer. Do not ask about a
value that a prototype or a measurement can decide.

## 4. Iterate

The user often challenges the proposal instead of answering a question. For
each challenge:

- Check the claim against the code or a measurement before you reply.
- If the user is right, change the proposal.
- If the user misses something, keep the proposal and show what they miss,
  with the evidence, in a few lines.

Keep each reply short. Send only what changed, not the full proposal again.

## 5. Close and implement

When the forks are settled, send the agreed design in a short form. Mark what
changed from your first proposal. Then wait for the user to say to implement.

While you implement, stop and ask before you do something that the user did
not agree to. This includes:

- A change to an agreed decision or to the scope.
- A change to what a test or a benchmark measures, or to the policy that it
  uses.
- A fix that a review finding asks for and that changes one of the items above.

## 6. Verify the change

The change is done only when the success check passes.

- **Bug fix:** the reproduction from step 1 becomes a test, or a test covers
  it. The test fails before the fix and passes after it.
- **Performance improvement:** run the same benchmark as in step 1. The metric
  must reach the target.

If the check does not pass, find out why first. Then report to the user the
result, the cause, and what it means for the design. Go back to step 2 with
the user. Do not change the design on your own to reach the target.
