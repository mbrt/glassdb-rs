#!/usr/bin/env python3
"""Prints, for each run and policy, the geometric mean over cells of the
geometric mean of the shape throughputs against `size` of the same run, for
each mode. Also prints the cell of hi mode with 8 databases and affinity 0.

Usage: mixruns.py <files> [--exclude=mode:affinity:databases ...]
"""

import json
import math
import sys
from collections import defaultdict

excluded = {
    tuple(arg.split("=", 1)[1].split(":"))
    for arg in sys.argv[1:]
    if arg.startswith("--exclude=")
}
files = [arg for arg in sys.argv[1:] if not arg.startswith("--")]
# rows[run][(mode, affinity, databases)][policy] = geometric mean of shapes
rows: dict[int, dict[tuple, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
policies: list[str] = []
for path in files:
    data = json.load(open(path))
    for index, run in enumerate(data["runs"]):
        for c in run["cells"]:
            shapes = c["shapes"]
            g = math.exp(sum(math.log(max(s["txPerSec"], 1e-3)) for s in shapes) / len(shapes))
            rows[index][(c["mode"], c["affinityPct"], c["databases"])][c["policy"]] = g
            if c["policy"] not in policies:
                policies.append(c["policy"])
for mode in ("lo", "hi"):
    print(f"{mode}:")
    for policy in policies:
        values = []
        for index in sorted(rows):
            ratios = [
                math.log(r[policy] / r["size"])
                for k, r in rows[index].items()
                if k[0] == mode
                and (k[0], str(k[1]), str(k[2])) not in excluded
                and policy in r
                and "size" in r
            ]
            values.append(math.exp(sum(ratios) / len(ratios)))
        key_cells = [rows[i][("hi", 0, 8)].get(policy) for i in sorted(rows)]
        key = " ".join(f"{v:5.1f}" for v in key_cells if v is not None)
        mean = math.exp(sum(math.log(v) for v in values) / len(values))
        runs = " ".join(f"{v:.3f}" for v in values)
        print(f"  {policy:34} mean={mean:.3f} runs=[{runs}] hi-a0-db8=[{key}]")
