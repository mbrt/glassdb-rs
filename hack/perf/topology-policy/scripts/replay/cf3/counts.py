#!/usr/bin/env python3
"""Prints the window counts of traced perfbench mixed cells: for each cell, the
committed and divided transactions by kind, the members per round, and the
split-side time, over all leaves and over the leaf windows that the avoidable
time shadow would split."""

import json
import sys
from pathlib import Path
from typing import Any

KINDS = ("direct", "locked", "readOnly")


def split_side(leaf: dict[str, Any]) -> float:
    return (
        leaf["lostCasMs"]
        + leaf["queueWaitMs"]
        + leaf["slowCasMs"]
        + leaf["inlinePressureMs"]
    )


def totals(leaves: list[dict[str, Any]]) -> str:
    committed = {k: sum(leaf["committed"][k] for leaf in leaves) for k in KINDS}
    divided = {k: sum(leaf["divided"][k] for leaf in leaves) for k in KINDS}
    rounds = sum(leaf["rounds"] for leaf in leaves)
    members = sum(leaf["roundMembers"] for leaf in leaves)
    txs = sum(committed.values())
    latency = sum(leaf["latencyMs"] for leaf in leaves)
    help_ms = sum(split_side(leaf) for leaf in leaves)
    parts = " ".join(
        f"{k}={divided[k]}/{committed[k]}" for k in KINDS if committed[k]
    )
    return (
        f"n={len(leaves):4} divided/committed {parts} "
        f"frac={sum(divided.values()) / max(txs, 1):.2f} "
        f"members/round={members / max(rounds, 1):.2f} rounds={rounds} "
        f"lat/tx={latency / max(txs, 1):.0f}ms help={help_ms:.0f}ms "
        f"help/tx={help_ms / max(txs, 1):.1f}ms"
    )


def summarize(cell: dict[str, Any]) -> None:
    trace = cell.get("shadow", {}).get("trace", [])
    seed = cell["seedLeafEntries"] or "def"
    rates = {s["shape"]: round(s["txPerSec"], 1) for s in cell["shapes"]}
    print(f"  seed={seed} {rates}")
    leaves = [leaf for window in trace for leaf in window["leaves"]]
    print(f"    all     {totals(leaves)}")
    wanted = [
        leaf
        for window in trace
        for leaf in window["leaves"]
        if split_side(leaf) > 0.25 * window["splitMs"]
    ]
    print(f"    over    {totals(wanted)}")


def main(paths: list[str]) -> None:
    for path in paths:
        report = json.loads(Path(path).read_text())
        print(f"== {Path(path).stem}")
        for run in report["runs"]:
            for cell in run["cells"]:
                summarize(cell)


if __name__ == "__main__":
    main(sys.argv[1:])
