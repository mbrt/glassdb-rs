#!/usr/bin/env python3
"""Replays the net time rule on the traced windows of unsplit perfbench mixed
cells, with two estimates of the time that a split adds: `count` is the
divided commits times the mean latency of the commits of the leaf, and `time`
is the time of the divided attempts, also of attempts that did not commit.

For each cell, prints the label (the geometric mean of the shape throughputs
of the tree that the setup split, against the unsplit tree), and for each
estimate, weight, and threshold, the leaves that the rule would split, and
when it would split the first one."""

import json
import math
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

HALF_LIFE = 10.0
THRESHOLDS_MS = (57.0, 125.0)
Leaf = dict[str, Any]


def split_side(leaf: Leaf) -> float:
    return (
        leaf["lostCasMs"]
        + leaf["queueWaitMs"]
        + leaf["slowCasMs"]
        + leaf["inlinePressureMs"]
    )


def by_count(leaf: Leaf) -> float:
    committed = sum(leaf["committed"].values())
    if committed == 0:
        return 0.0
    return sum(leaf["divided"].values()) * leaf["latencyMs"] / committed


def by_time(leaf: Leaf) -> float:
    return leaf["dividedTimeMs"]


ESTIMATES: dict[str, tuple[Callable[[Leaf], float], tuple[float, ...]]] = {
    "count": (by_count, (0.25, 1.0)),
    "time": (by_time, (0.05, 0.1, 0.25, 0.5)),
}


def replay(
    trace: list[dict[str, Any]],
    added: Callable[[Leaf], float],
    weight: float,
    threshold_ms: float,
) -> str:
    averages: dict[tuple[int, str], float] = {}
    split: set[tuple[int, str]] = set()
    first: float | None = None
    for window in sorted(trace, key=lambda window: window["atMs"]):
        decay = 0.5 ** (window["elapsedMs"] / 1000.0 / HALF_LIFE)
        db = window["database"]
        present = {leaf["leaf"]: leaf for leaf in window["leaves"]}
        for key in [k for k in averages if k[0] == db and k[1] not in present]:
            averages[key] *= decay
        for name, leaf in present.items():
            key = (db, name)
            net = split_side(leaf) - weight * added(leaf)
            averages[key] = averages.get(key, 0.0) * decay + net * (1 - decay)
            if key not in split and averages[key] > threshold_ms:
                split.add(key)
                if first is None:
                    first = window["atMs"] / 1000.0
    when = "-" if first is None else f"{first:.1f}s"
    return f"{len(split)}/{len(averages)}@{when}"


def geomean(cell: dict[str, Any]) -> float:
    rates = [max(shape["txPerSec"], 1e-3) for shape in cell["shapes"]]
    return math.exp(sum(math.log(rate) for rate in rates) / len(rates))


def main(paths: list[str]) -> None:
    columns = [
        (name, weight, threshold)
        for name, (_, weights) in ESTIMATES.items()
        for weight in weights
        for threshold in THRESHOLDS_MS
    ]
    header = " ".join(f"{n}{w}/{t:.0f}".ljust(12) for n, w, t in columns)
    print("cell".ljust(20) + "label ".ljust(7) + header)
    for path in paths:
        cells = json.loads(Path(path).read_text())["runs"][0]["cells"]
        unsplit = next(cell for cell in cells if not cell["seedLeafEntries"])
        seeded = next(cell for cell in cells if cell["seedLeafEntries"])
        label = geomean(seeded) / geomean(unsplit)
        trace = unsplit["shadow"]["trace"]
        row = " ".join(
            replay(trace, ESTIMATES[n][0], w, t).ljust(12) for n, w, t in columns
        )
        print(f"{Path(path).stem[3:]:<20}{label:<7.2f}{row}")


if __name__ == "__main__":
    main(sys.argv[1:])
