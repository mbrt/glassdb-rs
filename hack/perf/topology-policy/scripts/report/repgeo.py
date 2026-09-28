#!/usr/bin/env python3
"""Compares topology policies against `fixed` of the same run, over repeated runs.

Usage: repgeo.py <files>. Prints, for each cell, the median ratio to `fixed`
over the runs, and for each policy the geometric mean over cells of each run.
"""

import json
import math
import statistics
import sys
from collections import defaultdict

# (cell, policy) -> list of ratios, one per run.
ratios: dict[tuple, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
per_run: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
policies: list[str] = []
for path in sys.argv[1:]:
    data = json.load(open(path))
    for index, run in enumerate(data["runs"]):
        cells = {}
        for c in run["cells"]:
            cells[(c["workload"], c["leafMaxEntries"], c["databases"], c["policy"])] = c
        for (workload, leaves, dbs, policy), c in cells.items():
            if policy == "fixed":
                continue
            base = cells.get((workload, leaves, dbs, "fixed"))
            if not base or base["txPerSec"] <= 0:
                continue
            ratio = c["txPerSec"] / base["txPerSec"]
            ratios[(workload, leaves, dbs)][policy].append(ratio)
            per_run[policy][index].append(math.log(ratio))
            if policy not in policies:
                policies.append(policy)

print(f"{'cell':20}" + "".join(f"{p[:24]:>26}" for p in policies))
for cell in sorted(ratios):
    row = f"{cell[0]:9} L{cell[1]:<4} db{cell[2]:<3}"
    for p in policies:
        values = ratios[cell].get(p, [])
        if values:
            text = f"{statistics.median(values):.2f} [{min(values):.2f}-{max(values):.2f}]"
        else:
            text = "-"
        row += f"{text:>26}"
    print(row)
for p in policies:
    runs = [math.exp(sum(v) / len(v)) for _, v in sorted(per_run[p].items())]
    worst = min(
        (statistics.median(ratios[c][p]), c) for c in ratios if ratios[c].get(p)
    )
    print(
        f"{p:22} geomean per run: {' '.join(f'{r:.3f}' for r in runs)} "
        f"mean={statistics.mean(runs):.3f} worst cell median={worst[0]:.2f} {worst[1]}"
    )
