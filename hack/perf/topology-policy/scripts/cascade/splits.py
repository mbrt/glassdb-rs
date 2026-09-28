#!/usr/bin/env python3
"""Prints each split that the traced live policy asked for, with the leaf size
and the sums of the measurements of the leaf in the last 10 windows of the same
database instance before the split."""

import json
import sys
from collections import defaultdict, deque
from typing import Any

Leaf = dict[str, Any]


def short(leaf: str) -> str:
    parts = leaf.split("/")
    return "/".join(parts[-2:])[-18:]


def main(path: str) -> None:
    data = json.load(open(path))
    cell = data["runs"][0]["cells"][0]
    trace = sorted(cell["shadow"]["trace"], key=lambda w: w["atMs"])
    history: dict[tuple[int, str], deque[Leaf]] = defaultdict(lambda: deque(maxlen=10))
    splits = merges = 0
    print(f"{cell['policy']} restructure={cell['restructure']}")
    print(" at(s) db leaf               ent  lostCas qwait  divT  divConf  committed divided splitRecently")
    for window in trace:
        db = window["database"]
        leaves = {leaf["leaf"]: leaf for leaf in window["leaves"]}
        for name, leaf in leaves.items():
            history[(db, name)].append(leaf)
        for decision in window["decisions"][0]:
            kind, leaf_name = decision.split(" ")[:2]
            if kind == "merge":
                merges += 1
                continue
            splits += 1
            past = history[(db, leaf_name)]
            total = lambda field: sum(leaf[field] for leaf in past)
            committed = sum(sum(leaf["committed"].values()) for leaf in past)
            divided = sum(sum(leaf["divided"].values()) for leaf in past)
            entries = next((leaf["entries"] for leaf in reversed(past) if leaf["entries"]), None)
            recently = any(leaf["splitRecently"] for leaf in past)
            print(
                f"{window['atMs'] / 1000:6.1f} {db:2} {short(leaf_name):18} {entries!s:>4} "
                f"{total('lostCasMs'):8.0f} {total('queueWaitMs'):5.0f} {total('dividedTimeMs'):6.0f} "
                f"{total('dividedConflictTimeMs'):7.0f} {committed:9} {divided:7} {recently}"
            )
    print(f"split decisions {splits}, merge decisions {merges}")


if __name__ == "__main__":
    for path in sys.argv[1:]:
        main(path)
