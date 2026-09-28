"""Summarizes the r18 validation: the engine `avoidable` rule against `fixed`
(topology) and `size` (mixed), for each run."""

import collections
import glob
import json
import math


def geo(values):
    values = list(values)
    return math.exp(sum(math.log(v) for v in values) / len(values)) if values else float("nan")


def topology():
    for backend in ("s3", "gcs"):
        per_run = collections.defaultdict(list)
        churn = []
        for path in sorted(glob.glob(f"r18-topo-{backend}-L*.json")):
            for run in json.load(open(path))["runs"]:
                cells = run["cells"]
                key = lambda c: (c["workload"], c["leafMaxEntries"], c["databases"])
                base = {key(c): c["txPerSec"] for c in cells if c["policy"] == "fixed"}
                for c in cells:
                    if c["policy"] != "avoidable" or key(c) not in base:
                        continue
                    per_run[run["run"]].append((c["txPerSec"] / base[key(c)], key(c)))
                    if c["workload"] == "adjacent":
                        churn.append((run["run"], key(c), c["adaptSplits"], c["adaptMerges"],
                                      c["measuredSplits"], c["measuredMerges"],
                                      round(c["txPerSec"] / base[key(c)], 2)))
        for run, values in sorted(per_run.items()):
            worst = min(values)
            print(f"topology {backend} run={run} n={len(values)} geomean={geo(v for v, _ in values):.3f} "
                  f"worst={worst[0]:.2f} {worst[1]}")
        for row in churn:
            print("  adjacent", row)


def mixed():
    for backend in ("s3", "gcs"):
        ratios = collections.defaultdict(list)
        sums = collections.defaultdict(list)
        changed = collections.defaultdict(list)
        notes = []
        for path in sorted(glob.glob(f"r18-mixed-{backend}-db*.json")):
            for run in json.load(open(path))["runs"]:
                cells = run["cells"]
                key = lambda c: (c["mode"], c["databases"], c["affinityPct"])
                base = {key(c): c for c in cells if c["policy"] == "size"}
                for c in cells:
                    if c["policy"] != "avoidable" or key(c) not in base:
                        continue
                    b = {s["shape"]: s["txPerSec"] for s in base[key(c)]["shapes"]}
                    shapes = {s["shape"]: s["txPerSec"] for s in c["shapes"]}
                    cell = geo(shapes[s] / b[s] for s in shapes if b.get(s))
                    mode = c["mode"]
                    ratios[(mode, run["run"])].append(cell)
                    sums[(mode, run["run"])].append(sum(shapes.values()) / sum(b.values()))
                    r = c["restructure"]
                    changes = r["warmupSplits"] + r["warmupMerges"] + r["measuredSplits"] + r["measuredMerges"]
                    if mode == "hi" and changes:
                        changed[run["run"]].append(cell)
                    if mode == "hi" and c["databases"] == 8 and c["affinityPct"] in (0, 50):
                        notes.append((run["run"], c["affinityPct"], round(cell, 2),
                                      r["warmupSplits"], r["warmupMerges"],
                                      r["measuredSplits"], r["measuredMerges"]))
        for (mode, run), values in sorted(ratios.items()):
            print(f"mixed {backend} {mode} run={run} n={len(values)} geomean={geo(values):.3f} "
                  f"sum={geo(sums[(mode, run)]):.3f} min={min(values):.2f}")
        for run, values in sorted(changed.items()):
            print(f"  hi cells that changed the tree run={run}: {len(values)} geomean={geo(values):.3f}")
        for row in notes:
            print("  hi db8 (run, affinity, ratio, warmup splits, merges, measured splits, merges)", row)


topology()
mixed()
