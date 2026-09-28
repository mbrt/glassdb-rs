#!/usr/bin/env python3
"""Replays a moving average rule on the traced windows of unsplit perfbench
mixed cells. For each leaf of each database instance, the average of the net
time per window is `help - weight * divided * mean latency`, with a half-life.
A leaf splits when the average is more than 0.25 times the split time. Prints
the first split time of each cell, and how many leaves would split."""

import json
import sys
from pathlib import Path
from typing import Any

WEIGHTS = (0.0, 0.25, 0.5)
HALF_LIVES = (5.0, 10.0, 20.0)


def split_side(leaf: dict[str, Any]) -> float:
    return (
        leaf["lostCasMs"]
        + leaf["queueWaitMs"]
        + leaf["slowCasMs"]
        + leaf["inlinePressureMs"]
    )


def hurt(leaf: dict[str, Any]) -> float:
    committed = sum(leaf["committed"].values())
    if committed == 0:
        return 0.0
    return sum(leaf["divided"].values()) * leaf["latencyMs"] / committed


def replay(trace: list[dict[str, Any]], weight: float, half_life: float) -> str:
    averages: dict[tuple[int, str], float] = {}
    split: set[tuple[int, str]] = set()
    first: float | None = None
    for window in trace:
        decay = 0.5 ** (window["elapsedMs"] / 1000.0 / half_life)
        db = window["database"]
        present = {leaf["leaf"]: leaf for leaf in window["leaves"]}
        for key in [k for k in averages if k[0] == db and k[1] not in present]:
            averages[key] *= decay
        for name, leaf in present.items():
            key = (db, name)
            net = split_side(leaf) - weight * hurt(leaf)
            averages[key] = averages.get(key, 0.0) * decay + net * (1 - decay)
            if key not in split and averages[key] > 0.25 * window["splitMs"]:
                split.add(key)
                if first is None:
                    first = window["atMs"] / 1000.0
    when = "-" if first is None else f"{first:.1f}s"
    return f"{len(split)}/{len(averages)}@{when}"


def main(paths: list[str]) -> None:
    columns = [(w, h) for w in WEIGHTS for h in HALF_LIVES]
    print("cell".ljust(22) + " ".join(f"w{w}/h{h:<4}".ljust(13) for w, h in columns))
    for path in paths:
        report = json.loads(Path(path).read_text())
        for run in report["runs"]:
            for cell in run["cells"]:
                if cell["seedLeafEntries"]:
                    continue
                trace = cell["shadow"]["trace"]
                row = " ".join(replay(trace, w, h).ljust(13) for w, h in columns)
                print(f"{Path(path).stem[3:]:<22}{row}")


if __name__ == "__main__":
    main(sys.argv[1:])
