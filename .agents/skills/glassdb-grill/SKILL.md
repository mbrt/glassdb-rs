---
name: glassdb-grill
description: Stress-test a plan or design through a docs-aware interview. Use when the user wants to stress-test their thinking, or you need to uncover hidden assumptions, conflicts, or gaps in a plan or design.
---

Interview the user relentlessly until you reach a shared understanding. Map this as a **design tree**: every decision branches into the decisions that hang off it.

## Workflow

1. Start with stating your current understanding of the design (high level) and ask the user questions to navigate the tree
2. Ask questions to clarify
3. When settled (or asked to stop), state the current shared design, highlight changes from the initial understanding.

## Methodology

Terminology:

- A **round** is a batch of questions.
- The **frontier** is every decision whose prerequisites are already settled: the questions you can ask _now_ without guessing at answers you haven't heard yet.

When asking questions:

- Work the tree in rounds
- Ask the whole frontier in one round
- Wait for the user's answers before the next round

The session is complete either when:

- All questions are answered and the frontier is empty, or
- The user explicitly states that they are satisfied with the current understanding, or
- The value of asking more questions is low

### Computing the frontier

Each round the user answers reshapes the tree: settled decisions push the frontier outward and unblock questions that depended on them. Recompute the frontier and ask the next round. A question whose answer depends on another question still open in this round belongs to a _later_ round, not this one.

Make sure to sort questions by importance. Explore breadth before depth. Strive for getting the most value out of each round and minimize questions. 

### Questions

Make sure to:

- Find facts yourself through sub-agents when a question or recommendation requires them, don't ask the user
- Only ask legitimate forks in the design, not to confirm your preferred answer
- Don't ask questions that could be better answered by a prototype (e.g. the tweaking of a parameter). Defer the decision to a prototype or experimentation instead
- Include your recommended answer for each question
- Be concise but clear

## Formatting

Format a round like so:

```
[Estimated questions left: <total number>]

❓ **Q1** - **<question title>**: <question body, might be multiple paragraphs, including multiple choices>

➡️ <your recommended answer>

---

❓ **Q2** - **<question title>**: <question body, might be multiple paragraphs, including multiple choices>

➡️ <your recommended answer>
```
