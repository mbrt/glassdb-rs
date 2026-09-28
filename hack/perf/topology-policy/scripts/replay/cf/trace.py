#!/usr/bin/env python3
"""Prints the traced windows of perfbench mixed cells: the distribution of the
split-side time per leaf and window, and optionally the series of windows."""

import argparse
import json
from pathlib import Path
from typing import Any


def split_side(leaf: dict[str, Any]) -> float:
    return (
        leaf["lostCasMs"]
        + leaf["queueWaitMs"]
        + leaf["slowCasMs"]
        + leaf["inlinePressureMs"]
    )


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(fraction * len(ordered)))]


def summarize(cell: dict[str, Any], series: bool) -> None:
    trace = cell.get("shadow", {}).get("trace", [])
    seed = cell["seedLeafEntries"] or "def"
    rates = {s["shape"]: round(s["txPerSec"], 1) for s in cell["shapes"]}
    totals = [split_side(leaf) for window in trace for leaf in window["leaves"]]
    thresholds = sorted({round(w["splitMs"] * 0.25, 1) for w in trace})
    over = sum(1 for window in trace for leaf in window["leaves"]
               if split_side(leaf) > 0.25 * window["splitMs"])
    print(
        f"  seed={seed} {rates} windows={len(trace)} leafwindows={len(totals)} "
        f"threshold={thresholds} over={over} "
        f"split-side p10/50/90/max="
        f"{percentile(totals, 0.1):.0f}/{percentile(totals, 0.5):.0f}/"
        f"{percentile(totals, 0.9):.0f}/{max(totals, default=0):.0f} "
        f"sum={sum(totals):.0f}"
    )
    if not series:
        return
    for window in trace:
        leaves = " ".join(
            f"[{leaf['leaf'][-6:]} lost={leaf['lostCasMs']:.0f} "
            f"queue={leaf['queueWaitMs']:.0f} n={leaf['entries']}]"
            for leaf in window["leaves"]
        )
        print(
            f"    db{window['database']} t={window['atMs'] / 1000:5.1f}s "
            f"el={window['elapsedMs']:.0f} {leaves} -> {window['decisions']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--series", action="store_true")
    args = parser.parse_args()
    for path in args.paths:
        report = json.loads(Path(path).read_text())
        print(f"== {Path(path).stem}")
        for run in report["runs"]:
            for cell in run["cells"]:
                summarize(cell, args.series)


if __name__ == "__main__":
    main()
