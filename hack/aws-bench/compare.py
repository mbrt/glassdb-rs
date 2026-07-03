#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
#     "numpy>=1.26",
# ]
# ///
"""Compare two rtbench/autoresearch result sets and report how they differ.

Generic two-directory comparator. Each side is a directory of result files
produced by `rtbench` and (optionally) the `autoresearch` scoring harness:

* `throughput.csv`  -> transaction throughput per tx-type (total tx/s);
* `samples.csv`     -> per-transaction latency percentiles (p50/p90/p95);
* `stats.csv`       -> retries/tx and backend-ops/tx (object-storage round-trips);
* `deadlock.csv`    -> latency under contention (p50/p90 at 100% overlap);
* `score.json`      -> autoresearch primary score + per-workload cost/ops per tx;
* `mixbench.json`   -> mixed-workload grid: per-shape throughput and ops/tx across
                       contention mode x Database topology (the contention /
                       in-process-dedup efficiency signal).

Whatever files are present on both sides are compared; the rest are skipped.
Every metric is reported as the ratio ``b / a`` (the second set over the first),
so for an engine comparison with ``--label-a v1 --label-b v2`` a ratio above 1.0
means v2 has more of that quantity than v1:

* throughput ratio > 1  -> v2 is faster (good);
* latency / retries / backend-ops / cost ratio < 1 -> v2 is cheaper (good).

Two original use cases are both covered by this generic shape:

* engine versions: ``--a out/v1 --label-a v1 --b out/v2 --label-b v2`` (see
  ``compare-refs.sh``);
* fake vs real S3: ``--a out --label-a real --b out-fake --label-b fake``.

It also writes overlay PNGs (``cmp-tx-throughput.png``, ``cmp-tx-latency.png``,
``cmp-retries.png``, ``cmp-deadlock-latency.png``) into ``--out`` so the curves
can be eyeballed together.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

# Backend-op columns that sum into total round-trips, in case a `stats.csv` from
# an older run predates the explicit `backend-ops` total column. Engine versions
# categorize ops differently (e.g. v1's tag/metadata ops vs v2 folding all
# coordination into object reads/writes), so summing every class is what makes
# the efficiency number comparable across versions.
OP_COLS = ["obj-write", "obj-read", "obj-list", "meta-write", "meta-read"]

# autoresearch JSON op-count fields (camelCase) that sum into ops/tx.
SCORE_OP_FIELDS = ["objReads", "objWrites", "objLists", "metaReads", "metaWrites"]


def read_csv(input_dir: Path, name: str) -> pd.DataFrame | None:
    """Load a result CSV, accepting an optional ``.xz`` compression."""
    for path in (input_dir / name, input_dir / f"{name}.xz"):
        if path.exists():
            return pd.read_csv(path)
    return None


def read_json(input_dir: Path, name: str) -> Any | None:
    """Load a result JSON. The top-level shape depends on the file: `score.json`
    is an object (dict), `mixbench.json` is an array (list) of grid cells."""
    path = input_dir / name
    if path.exists():
        return json.loads(path.read_text())
    return None


def _ratio(b: float, a: float) -> float:
    return float("nan") if a == 0 else b / a


def _geomean(s: pd.Series) -> float:
    s = pd.Series(s).dropna()
    s = s[s > 0]
    if s.empty:
        return float("nan")
    return float(np.exp(np.log(s).mean()))


def backend_ops_series(df: pd.DataFrame) -> pd.Series:
    """Total backend round-trips per row: the `backend-ops` column if present,
    else the sum of whatever per-class op columns exist (back-compat)."""
    if "backend-ops" in df.columns:
        return df["backend-ops"]
    present = [c for c in OP_COLS if c in df.columns]
    return df[present].sum(axis=1) if present else pd.Series(0, index=df.index)


# ---------------------------------------------------------------------------
# Tables (each returns a merged frame with a `ratio` / `*-ratio` column)
# ---------------------------------------------------------------------------


def throughput_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Total tx/s per (concurrency, tx-type): num_db * median(per-db rate)."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        g = df.groupby(["num-db", "tx-type"])["tx-per-sec"].median().reset_index()
        g["total-tps"] = g["tx-per-sec"] * g["num-db"]
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = agg(a).merge(
        agg(b), on=["num-db", "tx-type", "concurrent"], suffixes=("_a", "_b")
    )
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["total-tps_b"], r["total-tps_a"]), axis=1
    )
    return merged


def latency_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """p50/p90/p95 transaction latency (ms) per (concurrency, tx-type)."""
    pctiles = {"p50": 0.5, "p90": 0.9, "p95": 0.95}

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for (numdb, tp), grp in df.groupby(["num-db", "tx-type"]):
            row = {"num-db": numdb, "tx-type": tp}
            for name, q in pctiles.items():
                row[name] = grp["latency"].quantile(q)
            rows.append(row)
        out = pd.DataFrame(rows)
        out["concurrent"] = out["num-db"] * conc_per_db
        return out

    merged = agg(a).merge(
        agg(b), on=["num-db", "tx-type", "concurrent"], suffixes=("_a", "_b")
    )
    for p in pctiles:
        merged[f"{p}-ratio"] = merged.apply(
            lambda r, p=p: _ratio(r[f"{p}_b"], r[f"{p}_a"]), axis=1
        )
    return merged


def retries_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        d["retries-per-tx"] = d["num-retries"] / d["num-tx"].where(d["num-tx"] > 0)
        g = d.groupby("num-db")["retries-per-tx"].median().reset_index()
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = agg(a).merge(agg(b), on=["num-db", "concurrent"], suffixes=("_a", "_b"))
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["retries-per-tx_b"], r["retries-per-tx_a"]), axis=1
    )
    return merged


def backend_ops_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Backend round-trips per committed transaction per concurrency step."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        d["backend-ops"] = backend_ops_series(d)
        g = (
            d.groupby("num-db")
            .agg({"backend-ops": "sum", "num-tx": "sum"})
            .reset_index()
        )
        g["ops-per-tx"] = g["backend-ops"] / g["num-tx"].where(g["num-tx"] > 0)
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = agg(a).merge(agg(b), on=["num-db", "concurrent"], suffixes=("_a", "_b"))
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["ops-per-tx_b"], r["ops-per-tx_a"]), axis=1
    )
    return merged


def deadlock_table(a: pd.DataFrame, b: pd.DataFrame):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = df[df["overlap-pct"] == 100]
        if d.empty:
            d = df
        g = (
            d.groupby("num-keys")["latency-ms"]
            .quantile([0.5, 0.9])
            .unstack()
            .reset_index()
        )
        g.columns = ["num-keys", "p50", "p90"]
        return g

    merged = agg(a).merge(agg(b), on="num-keys", suffixes=("_a", "_b"))
    for pct in ("p50", "p90"):
        merged[f"{pct}-ratio"] = merged.apply(
            lambda r, p=pct: _ratio(r[f"{p}_b"], r[f"{p}_a"]), axis=1
        )
    return merged


def efficiency_table(a: dict, b: dict):
    """Per-workload autoresearch cost/tx and ops/tx, plus the primary score."""

    def by_name(d: dict) -> dict:
        return {w["name"]: w for w in d.get("workloads", [])}

    wa, wb = by_name(a), by_name(b)
    rows = []
    for name in sorted(set(wa) & set(wb)):
        x, y = wa[name], wb[name]

        def ops_per_tx(w: dict) -> float:
            txn = w.get("txn", 0) or 0
            if txn == 0:
                return float("nan")
            return sum(w.get(f, 0) for f in SCORE_OP_FIELDS) / txn

        rows.append(
            {
                "workload": name,
                "costPerTx_a": x.get("costPerTx", float("nan")),
                "costPerTx_b": y.get("costPerTx", float("nan")),
                "cost-ratio": _ratio(y.get("costPerTx", 0), x.get("costPerTx", 0)),
                "opsPerTx_a": ops_per_tx(x),
                "opsPerTx_b": ops_per_tx(y),
                "ops-ratio": _ratio(ops_per_tx(y), ops_per_tx(x)),
            }
        )
    return pd.DataFrame(rows)


def _mixbench_cells(cells: list) -> dict:
    """Index a mixbench result grid by (mode, topology)."""
    return {(c["mode"], c["topology"]): c for c in cells}


def mixbench_shape_table(a: list, b: list) -> pd.DataFrame:
    """Per (mode, topology, shape) throughput, latency, and — where the topology
    attributes ops per shape (`per-shape`) — ops/tx and retries/tx ratios."""
    ca, cb = _mixbench_cells(a), _mixbench_cells(b)
    rows = []
    for key in sorted(set(ca) & set(cb)):
        mode, topo = key
        sa = {s["shape"]: s for s in ca[key].get("shapes", [])}
        sb = {s["shape"]: s for s in cb[key].get("shapes", [])}
        for shape in sorted(set(sa) & set(sb)):
            x, y = sa[shape], sb[shape]
            ox, oy = x.get("ops"), y.get("ops")
            rows.append(
                {
                    "mode": mode,
                    "topology": topo,
                    "shape": shape,
                    "tps_a": x.get("txPerSec", float("nan")),
                    "tps_b": y.get("txPerSec", float("nan")),
                    "tps-ratio": _ratio(y.get("txPerSec", 0), x.get("txPerSec", 0)),
                    "p50-ratio": _ratio(y.get("p50Ms", 0), x.get("p50Ms", 0)),
                    "p90-ratio": _ratio(y.get("p90Ms", 0), x.get("p90Ms", 0)),
                    "opsPerTx_a": ox.get("totalOpsPerTx") if ox else float("nan"),
                    "opsPerTx_b": oy.get("totalOpsPerTx") if oy else float("nan"),
                    "ops-ratio": (
                        _ratio(oy["totalOpsPerTx"], ox["totalOpsPerTx"])
                        if ox and oy
                        else float("nan")
                    ),
                    "retries-ratio": (
                        _ratio(oy["retriesPerTx"], ox["retriesPerTx"])
                        if ox and oy
                        else float("nan")
                    ),
                }
            )
    return pd.DataFrame(rows)


def mixbench_aggregate_table(a: list, b: list) -> pd.DataFrame:
    """Whole-DB aggregate ops/tx and retries/tx per (mode, topology), for cells
    (the `shared` topology) that cannot attribute ops per shape."""
    ca, cb = _mixbench_cells(a), _mixbench_cells(b)
    rows = []
    for key in sorted(set(ca) & set(cb)):
        mode, topo = key
        oa, ob = ca[key].get("aggregateOps"), cb[key].get("aggregateOps")
        if not (oa and ob):
            continue
        rows.append(
            {
                "mode": mode,
                "topology": topo,
                "opsPerTx_a": oa.get("totalOpsPerTx", float("nan")),
                "opsPerTx_b": ob.get("totalOpsPerTx", float("nan")),
                "ops-ratio": _ratio(
                    ob.get("totalOpsPerTx", 0), oa.get("totalOpsPerTx", 0)
                ),
                "retries-ratio": _ratio(
                    ob.get("retriesPerTx", 0), oa.get("retriesPerTx", 0)
                ),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def print_table(title: str, df: pd.DataFrame) -> None:
    print(f"\n## {title}\n")
    if df is None or df.empty:
        print("(no overlapping data)")
        return
    with pd.option_context(
        "display.max_rows",
        None,
        "display.width",
        220,
        "display.float_format",
        "{:.3f}".format,
    ):
        print(df.to_string(index=False))


def summarize(name: str, ratios: pd.Series) -> str:
    r = pd.Series(ratios).dropna()
    if r.empty:
        return f"{name}: no data"
    return (
        f"{name}: ratio b/a min={r.min():.2f} median={r.median():.2f} "
        f"max={r.max():.2f} (geomean={_geomean(r):.2f})"
    )


def append_summary(path: Path, title: str, summaries: list[str]) -> None:
    """Append a small markdown section for this comparison to ``path``.

    The shell driver points every comparison at the same file so the result is
    one compact, trackable digest per run (deterministic for the autoresearch
    score, noisy but indicative for the rtbench ratios)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"## {title or 'comparison'}", ""]
    if summaries:
        lines += [f"- {s}" for s in summaries]
    else:
        lines.append("- (no overlapping result files)")
    lines.append("")
    with path.open("a") as f:
        f.write("\n".join(lines) + "\n")


def _tidy_throughput(a, b, la, lb, conc_per_db):
    frames = []
    for src, df in ((la, a), (lb, b)):
        d = df.copy()
        d["concurrent"] = d["num-db"] * conc_per_db
        d["total-tps"] = d["tx-per-sec"] * d["num-db"]
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_latency(a, b, la, lb, conc_per_db):
    frames = []
    for src, df in ((la, a), (lb, b)):
        d = df.copy()
        d["concurrent"] = d["num-db"] * conc_per_db
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_retries(a, b, la, lb, conc_per_db):
    frames = []
    for src, df in ((la, a), (lb, b)):
        d = df.copy()
        d["concurrent"] = d["num-db"] * conc_per_db
        d["retries-per-tx"] = d["num-retries"] / d["num-tx"].where(d["num-tx"] > 0)
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_deadlock(a, b, la, lb):
    frames = []
    for src, df in ((la, a), (lb, b)):
        d = df[df["overlap-pct"] == 100].copy()
        if d.empty:
            d = df.copy()
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def plot_overlay_throughput(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="total-tps",
        hue="tx-type",
        style="source",
        estimator="median",
        errorbar=None,
        ax=ax,
    )
    ax.set_title("Transaction throughput")
    ax.set_xlabel("Concurrent transactions")
    ax.set_ylabel("Transactions / sec")
    _save(fig, out_dir, "cmp-tx-throughput.png")


def plot_overlay_latency(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="latency",
        hue="tx-type",
        style="source",
        estimator="median",
        errorbar=None,
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Transaction latency (p50)")
    ax.set_xlabel("Concurrent transactions")
    ax.set_ylabel("Latency (ms, log scale)")
    _save(fig, out_dir, "cmp-tx-latency.png")


def plot_overlay_retries(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="concurrent",
        y="retries-per-tx",
        style="source",
        estimator="median",
        errorbar=None,
        marker="o",
        ax=ax,
    )
    ax.set_title("Transaction retries")
    ax.set_xlabel("Concurrent transactions")
    ax.set_ylabel("Retries per transaction")
    _save(fig, out_dir, "cmp-retries.png")


def plot_overlay_deadlock(data, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=data,
        x="num-keys",
        y="latency-ms",
        style="source",
        estimator="median",
        errorbar=("pi", 80),
        marker="o",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Latency under contention")
    ax.set_xlabel("Contended keys (5 workers, 100% overlap)")
    ax.set_ylabel("Transaction latency (ms, log scale)")
    _save(fig, out_dir, "cmp-deadlock-latency.png")


def _save(fig: plt.Figure, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=120, bbox_inches="tight")
    print(f"wrote {path}")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent
    parser.add_argument("--a", type=Path, default=base / "out")
    parser.add_argument("--b", type=Path, default=base / "out-fake")
    parser.add_argument("--label-a", default="a")
    parser.add_argument("--label-b", default="b")
    parser.add_argument(
        "--out", type=Path, default=None, help="dir for PNGs (default: --b)"
    )
    parser.add_argument("--title", default="", help="prefix for the report header")
    parser.add_argument("--concurrency-per-db", type=int, default=10)
    parser.add_argument("--no-plots", action="store_true", help="skip overlay PNGs")
    parser.add_argument(
        "--summary-out",
        type=Path,
        default=None,
        help="append the compact ratio summary as a markdown section to this file",
    )
    args = parser.parse_args()

    la, lb = args.label_a, args.label_b
    out_dir = args.out if args.out is not None else args.b
    cpd = args.concurrency_per_db

    sns.set_theme(style="whitegrid", context="talk")
    prefix = f"{args.title}: " if args.title else ""
    print(f"# {prefix}comparison: a={la} ({args.a})  b={lb} ({args.b})")
    print(f"# ratio = {lb} / {la}")

    summaries: list[str] = []

    a_tp, b_tp = read_csv(args.a, "throughput.csv"), read_csv(args.b, "throughput.csv")
    if a_tp is not None and b_tp is not None:
        tbl = throughput_table(a_tp, b_tp, cpd)
        cols = ["concurrent", "tx-type", "total-tps_a", "total-tps_b", "ratio"]
        print_table(f"Throughput (total tx/s, {lb}/{la})", tbl[cols])
        for tx_type, grp in tbl.groupby("tx-type"):
            summaries.append(summarize(f"throughput[{tx_type}]", grp["ratio"]))

    a_la, b_la = read_csv(args.a, "samples.csv"), read_csv(args.b, "samples.csv")
    if a_la is not None and b_la is not None:
        tbl = latency_table(a_la, b_la, cpd)
        cols = [
            "concurrent",
            "tx-type",
            "p50_a",
            "p50_b",
            "p50-ratio",
            "p90-ratio",
            "p95-ratio",
        ]
        print_table(
            f"Latency (ms; p50 values + percentile {lb}/{la} ratios)", tbl[cols]
        )
        for tx_type, grp in tbl.groupby("tx-type"):
            summaries.append(summarize(f"latency-p50[{tx_type}]", grp["p50-ratio"]))

    a_st, b_st = read_csv(args.a, "stats.csv"), read_csv(args.b, "stats.csv")
    if a_st is not None and b_st is not None:
        tbl = retries_table(a_st, b_st, cpd)
        cols = ["concurrent", "retries-per-tx_a", "retries-per-tx_b", "ratio"]
        print_table(f"Retries per transaction ({lb}/{la})", tbl[cols])
        summaries.append(summarize("retries", tbl["ratio"]))

        tbl = backend_ops_table(a_st, b_st, cpd)
        cols = ["concurrent", "ops-per-tx_a", "ops-per-tx_b", "ratio"]
        print_table(f"Backend round-trips per transaction ({lb}/{la})", tbl[cols])
        summaries.append(summarize("backend-ops/tx", tbl["ratio"]))

    a_dl, b_dl = read_csv(args.a, "deadlock.csv"), read_csv(args.b, "deadlock.csv")
    if a_dl is not None and b_dl is not None:
        tbl = deadlock_table(a_dl, b_dl)
        print_table(f"Deadlock latency at 100% overlap (ms, {lb}/{la})", tbl)
        summaries.append(summarize("deadlock-p50", tbl["p50-ratio"]))
        summaries.append(summarize("deadlock-p90", tbl["p90-ratio"]))

    a_sc, b_sc = read_json(args.a, "score.json"), read_json(args.b, "score.json")
    if a_sc is not None and b_sc is not None:
        sa, sb = a_sc.get("score"), b_sc.get("score")
        if sa is not None and sb is not None:
            print("\n## Autoresearch primary score (cost/tx geomean, lower = better)\n")
            print(
                f"{la}={sa:.2f}  {lb}={sb:.2f}  ratio({lb}/{la})={_ratio(sb, sa):.3f}"
            )
            summaries.append(
                f"autoresearch-score: {la}={sa:.2f} {lb}={sb:.2f} "
                f"ratio={_ratio(sb, sa):.3f}"
            )
        tbl = efficiency_table(a_sc, b_sc)
        cols = [
            "workload",
            "costPerTx_a",
            "costPerTx_b",
            "cost-ratio",
            "opsPerTx_a",
            "opsPerTx_b",
            "ops-ratio",
        ]
        print_table(f"Autoresearch per-workload cost/ops per tx ({lb}/{la})", tbl[cols])
        if not tbl.empty:
            summaries.append(summarize("autoresearch-cost/tx", tbl["cost-ratio"]))
            summaries.append(summarize("autoresearch-ops/tx", tbl["ops-ratio"]))

    a_mx, b_mx = read_json(args.a, "mixbench.json"), read_json(args.b, "mixbench.json")
    if a_mx is not None and b_mx is not None:
        tbl = mixbench_shape_table(a_mx, b_mx)
        if not tbl.empty:
            cols = [
                "mode",
                "topology",
                "shape",
                "tps_a",
                "tps_b",
                "tps-ratio",
                "p50-ratio",
                "opsPerTx_a",
                "opsPerTx_b",
                "ops-ratio",
                "retries-ratio",
            ]
            print_table(f"mixbench per-shape ({lb}/{la})", tbl[cols])
            # Throughput ratio per shape (geomean folds the mode/topology cells).
            for shape, grp in tbl.groupby("shape"):
                summaries.append(summarize(f"mix-tps[{shape}]", grp["tps-ratio"]))
            # ops/tx + retries/tx are per-shape only where a shape owns its DBs
            # (the `per-shape` topology); keep the mode split so the hi-contention
            # dedup signal is not washed out.
            ops = tbl.dropna(subset=["ops-ratio"])
            for (mode, shape), grp in ops.groupby(["mode", "shape"]):
                summaries.append(
                    summarize(f"mix-ops/tx[{mode}/{shape}]", grp["ops-ratio"])
                )
            for mode, grp in ops.groupby("mode"):
                summaries.append(
                    summarize(f"mix-retries/tx[{mode}]", grp["retries-ratio"])
                )
        agg = mixbench_aggregate_table(a_mx, b_mx)
        if not agg.empty:
            print_table(f"mixbench shared-DB aggregate ops/tx ({lb}/{la})", agg)
            for mode, grp in agg.groupby("mode"):
                summaries.append(summarize(f"mix-agg-ops/tx[{mode}]", grp["ops-ratio"]))

    print("\n## Summary (ratio = b/a; throughput >1 good, latency/ops/cost <1 good)\n")
    for s in summaries:
        print(f"- {s}")
    if not summaries:
        print("(no overlapping result files found on both sides)")

    if args.summary_out is not None:
        append_summary(args.summary_out, args.title, summaries)

    if not args.no_plots:
        if a_tp is not None and b_tp is not None:
            plot_overlay_throughput(_tidy_throughput(a_tp, b_tp, la, lb, cpd), out_dir)
        if a_la is not None and b_la is not None:
            # p50 latency per (concurrent, tx-type) for the overlay.
            lat = latency_table(a_la, b_la, cpd)
            tidy = pd.concat(
                [
                    lat[["concurrent", "tx-type", "p50_a"]]
                    .rename(columns={"p50_a": "latency"})
                    .assign(source=la),
                    lat[["concurrent", "tx-type", "p50_b"]]
                    .rename(columns={"p50_b": "latency"})
                    .assign(source=lb),
                ],
                ignore_index=True,
            )
            plot_overlay_latency(tidy, out_dir)
        if a_st is not None and b_st is not None:
            plot_overlay_retries(_tidy_retries(a_st, b_st, la, lb, cpd), out_dir)
        if a_dl is not None and b_dl is not None:
            plot_overlay_deadlock(_tidy_deadlock(a_dl, b_dl, la, lb), out_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
