#!/usr/bin/env python3
"""Prints the geometric mean of the shape throughputs of mixed cells, per policy.

Usage: mixgeo.py <label=files> ... . Each argument names a source label and a
comma-separated list of JSON files. A column is `policy@label`. The last lines
give the geometric mean of each column against `size` of the same label, for
each mode.
"""

import json
import math
import sys
from collections import defaultdict

rows: dict[tuple, dict[str, float]] = defaultdict(dict)
columns: list[str] = []
for arg in sys.argv[1:]:
    label, files = arg.split("=", 1)
    for path in files.split(","):
        data = json.load(open(path))
        for run in data["runs"]:
            for c in run["cells"]:
                shapes = c["shapes"]
                g = math.exp(
                    sum(math.log(max(s["txPerSec"], 1e-3)) for s in shapes) / len(shapes)
                )
                star = "" if all(s["converged"] for s in shapes) else "*"
                column = f"{c['policy']}@{label}"
                rows[(c["mode"], c["affinityPct"], c["databases"])][column] = (g, star)
                if column not in columns:
                    columns.append(column)
print(f"{'cell':14}" + "".join(f"{c[:22]:>23}" for c in columns))
for key in sorted(rows):
    row = rows[key]
    text = "".join(
        f"{row[c][0]:22.1f}{row[c][1] or ' '}" if c in row else f"{'-':>23}" for c in columns
    )
    print(f"{key[0]:3} a{key[1]:<3} db{key[2]:<3} {text}")
for mode in ("lo", "hi"):
    parts = []
    for c in columns:
        base = "size@" + c.split("@", 1)[1]
        ratios = [
            math.log(r[c][0] / r[base][0])
            for k, r in rows.items()
            if k[0] == mode and c in r and base in r
        ]
        if ratios:
            parts.append(f"{c}={math.exp(sum(ratios) / len(ratios)):.3f}(n={len(ratios)})")
    print(mode, "against size:", " ".join(parts))
