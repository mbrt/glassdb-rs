#!/usr/bin/env python3
"""Prints mixed cells as rows of mode, affinity, and databases, with one column per policy.

Usage: mixtable.py <files> [policies]. A cell shows the total tx/s, the read-write
tx/s, and a star when a shape did not converge.
"""

import json
import sys
from collections import defaultdict

cells: dict[tuple, dict[str, list[str]]] = defaultdict(dict)
order: list[str] = []
for path in sys.argv[1].split(","):
    data = json.load(open(path))
    for run in data["runs"]:
        for c in run["cells"]:
            key = (c["mode"], c["affinityPct"], c["databases"])
            shapes = {s["shape"]: s for s in c["shapes"]}
            total = sum(s["txPerSec"] for s in shapes.values())
            rw = sum(shapes[n]["txPerSec"] for n in ("rwSingle", "rwMany") if n in shapes)
            star = "" if all(s["converged"] for s in shapes.values()) else "*"
            r = c["restructure"]
            text = f"{total:6.1f}/{rw:5.1f}{star:1}({r['measuredSplits']}/{r['measuredMerges']})"
            cells[key].setdefault(c["policy"], []).append(text)
            if c["policy"] not in order:
                order.append(c["policy"])
policies = sys.argv[2].split(",") if len(sys.argv) > 2 else order
print(f"{'cell':14}" + "".join(f"{p[:24]:>26}" for p in policies))
for key in sorted(cells):
    mode, aff, dbs = key
    for i in range(max(len(v) for v in cells[key].values())):
        row = f"{mode:3} a{aff:<3} db{dbs:<3} "
        for p in policies:
            values = cells[key].get(p, [])
            row += f"{values[i] if i < len(values) else '-':>26}"
        print(row)
