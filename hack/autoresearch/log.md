# Autoresearch experiment log

The running record of autoresearch experiments. The agent appends one entry per
experiment (kept or discarded) following the format in
[`program.md`](program.md). Autoresearch experiments do not get ADRs; this log
is the record of what was tried and why.

Each entry looks like:

```markdown
## <n>. <short title> - KEPT | DISCARDED
- Hypothesis: <what you expected and why>
- Change: <files / approach>
- Correctness: fast gate <pass/fail>, judge <approved/rejected[: reason]>
- Primary: <best> -> <new> (<+/-%>)
- Secondary: alloc <..>, ns <..>, cpu <..>
- Outcome & why: <why kept or discarded; what was learned>
- Commit: <hash if kept>
```

---

<!-- Experiment entries are appended below. -->

## 0. Baseline - KEPT
- Hypothesis: n/a (establish the starting point).
- Change: none; recorded the baseline correctness and score.
- Correctness: full gate (`check.sh --full`) PASS (build + make test + sim-test + 120s concurrent_tx fuzz, 6327 runs, no crash).
- Primary: baseline score = 434.95 (per-run: 434.83, 434.95, 435.00).
- Secondary: allocBytesPerTx=80409, allocsPerTx=907.2, nsPerTx=41129, cpuNsPerTx=62559.
- Per-workload cost/tx: singleRMW=72.07 (objW 1.01, metaR 0.015, metaW 0.02), multiRMW10=1082.6 (objW 11, metaW 9.8, objR 0.1, metaR 0.1), batchRead10=313.66 (metaR 10, objW 0.05), batchWrite100=20191.38 (objW 199, metaR 200, metaW ~2), readRepeat=31.51 (metaR 1, objW 0.005).
- Outcome & why: starting point. Biggest geomean lever is batchWrite100 (metaR 200/tx = 2 per created key, the double get_metadata called out in program.md), then multiRMW10 (locks+value-applies) and batchRead10 (10 metaR/tx for read validation).
- Commit: d87402c (pre-existing HEAD)

