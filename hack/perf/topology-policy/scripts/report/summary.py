#!/usr/bin/env python3
"""Prints one line for each cell of perfbench topology or mixed JSON files."""

import json
import sys


def topology(path: str, data: dict) -> None:
    for run in data["runs"]:
        for c in run["cells"]:
            t = c["perTx"]
            print(
                f"{path:28} {c['workload']:9} L{c['leafMaxEntries']:<4} db{c['databases']} "
                f"{c['policy']:18} tx/s={c['txPerSec']:7.1f} p50={c['p50Ms']:6.0f} "
                f"p90={c['p90Ms']:6.0f} ad={c['adaptSplits']:>3}s/{c['adaptMerges']:<3}m "
                f"meas={c['measuredSplits']}s/{c['measuredMerges']}m "
                f"leaves={c.get('finalLeaves', '-')!s:>4} "
                f"lost={t['avoidableLostCasMs']:6.1f} scan={t['avoidableScanCrossingMs']:6.1f} "
                f"adj={t['avoidableAdjacentMissMs']:6.1f} ops={t['backendOps']:.2f} "
                f"repl={t['replays']:.2f}"
            )


def mixed(path: str, data: dict) -> None:
    for c in data.get("cells", []):
        print(path, json.dumps(c)[:400])


for path in sys.argv[1:]:
    with open(path) as f:
        data = json.load(f)
    if data.get("scenario") == "topology":
        topology(path, data)
    else:
        mixed(path, data)
