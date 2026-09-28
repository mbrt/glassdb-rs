#!/usr/bin/env python3
"""Replays proportion rules on the traced windows of unsplit perfbench mixed
cells: for each rule, the share of leaf windows where it would split, next to
the avoidable time rule. The hurt of a split is the divided transactions times
the mean latency of the transactions of the leaf, times a weight."""

import json
import sys
from pathlib import Path
from typing import Any

WEIGHTS = (0.0, 0.25, 0.5, 1.0, 2.0)


def split_side(leaf: dict[str, Any]) -> float:
    return (
        leaf["lostCasMs"]
        + leaf["queueWaitMs"]
        + leaf["slowCasMs"]
        + leaf["inlinePressureMs"]
    )


def hurt(leaf: dict[str, Any]) -> float:
    committed = sum(leaf["committed"].values())
    divided = sum(leaf["divided"].values())
    if committed == 0:
        return 0.0
    return divided * leaf["latencyMs"] / committed


def splits(trace: list[dict[str, Any]], weight: float) -> int:
    return sum(
        1
        for window in trace
        for leaf in window["leaves"]
        if split_side(leaf) - weight * hurt(leaf) > 0.25 * window["splitMs"]
    )


def sustained(trace: list[dict[str, Any]], weight: float) -> str:
    """Returns the leaves whose net time over all their windows pays for a
    split in each window, of the leaves of all databases."""
    net: dict[tuple[int, str], float] = {}
    windows: dict[int, int] = {}
    threshold = 0.25 * trace[0]["splitMs"]
    for window in trace:
        windows[window["database"]] = windows.get(window["database"], 0) + 1
        for leaf in window["leaves"]:
            key = (window["database"], leaf["leaf"])
            net[key] = net.get(key, 0.0) + split_side(leaf) - weight * hurt(leaf)
    paying = sum(1 for (db, _), time in net.items() if time > threshold * windows[db])
    return f"{paying}/{len(net)}"


def margin(trace: list[dict[str, Any]], weight: float) -> float:
    """Returns the largest net time of a leaf over its windows, as a multiple
    of the split threshold over the same windows."""
    net: dict[tuple[int, str], float] = {}
    windows: dict[int, int] = {}
    threshold = 0.25 * trace[0]["splitMs"]
    for window in trace:
        windows[window["database"]] = windows.get(window["database"], 0) + 1
        for leaf in window["leaves"]:
            key = (window["database"], leaf["leaf"])
            net[key] = net.get(key, 0.0) + split_side(leaf) - weight * hurt(leaf)
    return max(time / (threshold * windows[db]) for (db, _), time in net.items())


def main(paths: list[str]) -> None:
    if paths[0] == "--margin":
        weights = (0.0, 0.1, 0.2, 0.25, 0.3, 0.4, 0.5)
        print("cell".ljust(24) + " ".join(f"w={w:<5}" for w in weights))
        for path in paths[1:]:
            report = json.loads(Path(path).read_text())
            for run in report["runs"]:
                for cell in run["cells"]:
                    if cell["seedLeafEntries"]:
                        continue
                    trace = cell["shadow"]["trace"]
                    row = " ".join(f"{margin(trace, w):<7.2f}" for w in weights)
                    print(f"{Path(path).stem:<24}{row}")
        return
    header = " ".join(f"w={w:<5}" for w in WEIGHTS)
    print("cell".ljust(24) + header + "  sustained " + header)
    for path in paths:
        report = json.loads(Path(path).read_text())
        for run in report["runs"]:
            for cell in run["cells"]:
                if cell["seedLeafEntries"]:
                    continue
                trace = cell["shadow"]["trace"]
                leaf_windows = max(sum(len(w["leaves"]) for w in trace), 1)
                shares = " ".join(
                    f"{splits(trace, w) / leaf_windows:<7.2f}" for w in WEIGHTS
                )
                paying = " ".join(f"{sustained(trace, w):<7}" for w in WEIGHTS)
                print(f"{Path(path).stem:<24}{shares}           {paying}")


if __name__ == "__main__":
    main(sys.argv[1:])
