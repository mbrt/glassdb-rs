#!/usr/bin/env python3
"""Prints the counterfactual labels of perfbench mixed runs: for each cell, the
throughput of each shape, the avoidable time, and what the shadow policies
would decide on its windows."""

import json
import math
import sys
from pathlib import Path
from typing import Any


def geomean(values: list[float]) -> float:
    if not values or min(values) <= 0:
        return 0.0
    return math.exp(sum(math.log(v) for v in values) / len(values))


def describe(cell: dict[str, Any]) -> str:
    shapes = {s["shape"]: s["txPerSec"] for s in cell["shapes"]}
    rates = " ".join(f"{name}={rate:7.1f}" for name, rate in shapes.items())
    avoid = cell["avoidablePerTx"]
    split_ms = (
        avoid["lostCasMs"]
        + avoid["queueWaitMs"]
        + avoid["slowCasMs"]
        + avoid["inlinePressureMs"]
    )
    merge_ms = avoid["adjacentMissMs"] + avoid["scanCrossingMs"]
    protocol = cell["aggregateProtocol"]
    shadow = cell.get("shadow") or {}
    windows = max(shadow.get("windows", 0), 1)
    decisions = " ".join(
        f"{p['policy']}:s{p['splits'] / windows:.2f}/m{p['merges'] / windows:.2f}"
        for p in shadow.get("policies", [])
    )
    seed = cell["seedLeafEntries"] or "def"
    return (
        f"seed={seed:>3} {rates} sum={sum(shapes.values()):7.1f} "
        f"geo={geomean(list(shapes.values())):6.1f} "
        f"split={split_ms:5.2f}ms(lost={avoid['lostCasMs']:.2f} "
        f"queue={avoid['queueWaitMs']:.2f} slow={avoid['slowCasMs']:.2f}) "
        f"merge={merge_ms:5.2f}ms land={protocol['directLandRate']:.2f} "
        f"win={shadow.get('windows', 0)} leafwin={shadow.get('leafWindows', 0)} "
        f"{decisions}"
    )


def main(paths: list[str]) -> None:
    for path in paths:
        report = json.loads(Path(path).read_text())
        print(f"== {Path(path).stem}")
        for run in report["runs"]:
            for cell in run["cells"]:
                print("  " + describe(cell))


if __name__ == "__main__":
    main(sys.argv[1:])
