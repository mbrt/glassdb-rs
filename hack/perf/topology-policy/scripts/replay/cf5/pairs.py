#!/usr/bin/env python3
"""For the cells of traced runs, prints the totals over all windows of each
collection: the split-side time of its leaves, the divided conflict time and
divided time of its leaves, and the merge-side time of its pairs, in seconds
per collection and per window."""

import json
import sys
from collections import defaultdict


def collection(leaf: str) -> str:
    return leaf.split("/_n/")[0].split("/_r")[0]


def main(path: str) -> None:
    data = json.load(open(path))
    for cell in data["runs"][0]["cells"]:
        trace = cell["shadow"]["trace"]
        totals: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        leaves: dict[str, set[str]] = defaultdict(set)
        for window in trace:
            for leaf in window["leaves"]:
                c = collection(leaf["leaf"])
                leaves[c].add(leaf["leaf"])
                t = totals[c]
                t["split"] += leaf["lostCasMs"] + leaf["queueWaitMs"] + leaf["slowCasMs"] + leaf["inlinePressureMs"]
                t["lost"] += leaf["lostCasMs"]
                t["divConf"] += leaf["dividedConflictTimeMs"]
                t["divT"] += leaf["dividedTimeMs"]
                t["commits"] += sum(leaf["committed"].values())
            for pair in window["pairs"]:
                c = collection(pair["left"])
                totals[c]["merge"] += pair["adjacentMissMs"] + pair["scanCrossingMs"]
        windows = len(trace) / max(1, cell["databases"])
        agg: dict[str, float] = defaultdict(float)
        for t in totals.values():
            for k, v in t.items():
                agg[k] += v
        n = max(1, len(totals))
        nleaves = sum(len(v) for v in leaves.values()) / n
        print(
            f"{path.split('/')[-1]:28} seed={cell['seedLeafEntries']!s:5} leaves/coll={nleaves:4.1f} "
            f"windows/db={windows:5.1f} ms per collection per window: "
            + " ".join(f"{k}={agg[k] / n / windows:7.1f}" for k in ["split", "lost", "divT", "divConf", "merge", "commits"])
        )


if __name__ == "__main__":
    for path in sys.argv[1:]:
        main(path)
