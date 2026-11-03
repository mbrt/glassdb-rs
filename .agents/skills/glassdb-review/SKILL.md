---
name: glassdb-review
description: Review a GlassDB change against the project standards, at a depth that matches its risk. Use when the user asks to review a change, commit, branch, or PR, or when AGENTS.md asks for a review.
argument-hint: "[target] [--tier protocol|design|tests|light]"
---

# Review a change

Review the target, and report findings. Do not fix them, commit, or open a PR.
The standards live in
[`docs/guides/change-standards.md`](../../../docs/guides/change-standards.md)
and the [design principles](../../../docs/principles.md). Do not restate them
in a review. Link to their sections.

## Inputs

- **Target:** a commit, a range `A..B`, a branch, a PR number, or paths. If
  there is no target, review the uncommitted changes and the branch against
  its merge-base with `main`.
- **`--tier`:** use this tier and do not classify the change.
- **Goal (optional):** what the change must do, from the issue or the PR
  description.
- **Agreed decisions (optional):** decisions that the user agreed to. Do not
  argue against them.
- **Data provenance (optional):** how and when benchmark results, logs, or
  test outputs were produced.
- **Commands (optional):** whether you can build or run tests. If the input
  says no, do not run them, because a benchmark or a measurement is running.

Read the diff before the goal and the PR description. Judge the change on its
own. If the context gives the author's arguments for why the change is correct,
or calls a workaround "verified", check the claim. Do not accept it.

## Classify the tier

Read the diff and pick the tier of the riskiest part. Classify by what the
change does, not only by its paths. If you are not sure, pick the higher tier.

| Tier | The change | Path hints |
|---|---|---|
| `protocol` | Changes the semantics of the transaction algorithm, locks, transaction records, splits and merges, the cache, evidence types, conditional backend operations, or the on-the-wire format. | `glassdb-trans/src/{algo,wound_wait,tlocker,node_locking,leaf_coord,collection_commit,gc,structural}`, `glassdb-storage/src/{cached_store,timeline,lock,txrecord,transaction,disk_cache,structural_intent,leaf,node,wire_id,wire_size}`, `glassdb-proto`, `glassdb-data`, `glassdb-backend*` |
| `design` | Adds or changes interfaces, modules, ownership of state, public API, configuration, or behavior outside the protocol. | New files, changed `pub` items, `glassdb/src`, changed dependencies between crates |
| `tests` | Changes only tests and test helpers. | `tests`, `sim`, `#[cfg(test)]` modules |
| `light` | Changes no behavior (deletions of unused code, renames, moves, formatting), or changes only benchmarks, CI, tools, or docs. | `glassdb-bench-scale`, `hack/`, `.github/`, `docs/`, `*.md`, `Makefile` |

A deletion or rename inside a `protocol` path is `light` if it changes no
behavior. An exception: a change to `timeline.rs` or `cached_store/evidence.rs`
that moves code or changes visibility, exports, or conversions stays
`protocol`.

## Run the review

Before the review, run the commands in
[Verification](../../../docs/guides/change-standards.md#verification) that
apply to the change, and record the results. If the target is not checked out,
for example a PR or an older commit, do not run them on the working tree. Say
in the report that no commands ran.

Each tier sets the sections of `change-standards.md` to check. Read the guides
that those sections link to. Add the sections of the lower tiers that the
change also touches. For example, a `design` change that also edits docs adds
Docs.

| Tier | Sections |
|---|---|
| `protocol` | Two passes. First pass: Protocol and the design principles. Second pass: the `design` sections and the lower-tier sections. |
| `design` | Ownership, Interfaces, Code, Tests |
| `tests` | Tests, Verification |
| `light` | Docs, Verification, and Code and Tests for benchmark code. Keep the review short. |

Check each finding against the code before you report it.

## Report the findings

Each finding has:

- **Severity:** `blocker` (wrong behavior, or a broken standard that is
  expensive to fix later), `should-fix`, or `nit`.
- **Location:** `file:line`.
- **Standard:** the section of `change-standards.md` or the principle that the
  change breaks.
- **Failure scenario:** the inputs or state that cause the wrong result. For a
  design finding, the change that the problem makes harder.
- **Fix (optional):** the problem comes first. You can propose a fix. You do
  not have to.

Drop a finding that has no failure scenario. Report a risk with no known
failure mechanism as a question, not as a finding.

The report starts with the tier and the reason for it, and the verification
commands with their results. Then the findings follow, most severe first.
