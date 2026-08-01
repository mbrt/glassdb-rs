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
* `deadlock-stats.csv` -> completion/retry/direct-path/drain metrics per cell;
* `inline-pressure.csv` -> direct-commit recovery after demand-driven splits;
* `score.json`      -> autoresearch primary score + per-workload cost/ops per tx;
* `mixbench.json`   -> mixed-workload grid: per-shape throughput and ops/tx across
                       contention mode x Database topology (the contention /
                       in-process-dedup efficiency signal).
* `diagnostics/metrics.csv` -> opt-in backend-role and protocol counters.

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
import csv
import json
import lzma
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
            # ADR-030-era rtbench wrote eight stats fields under a six-field
            # header. pandas otherwise consumes the leading fields as an index
            # and silently shifts every metric, including the concurrency key.
            # Repair only that exact historical shape.
            if name == "stats.csv":
                opener = lzma.open if path.suffix == ".xz" else open
                with opener(path, "rt", newline="") as f:
                    rows = csv.reader(f)
                    header = next(rows, [])
                    first = next(rows, [])
                if len(header) == 6 and len(first) == 8:
                    names = header + ["obj-list", "backend-ops"]
                    return pd.read_csv(path, skiprows=1, names=names)
            return pd.read_csv(path)
    return None


def read_json(input_dir: Path, name: str) -> Any | None:
    """Load a result JSON. The top-level shape depends on the file: `score.json`
    is an object (dict), `mixbench.json` is an array (list) of grid cells."""
    path = input_dir / name
    if path.exists():
        return json.loads(path.read_text())
    return None


def normalize_rtbench_time(
    df: pd.DataFrame | None, name: str, factor: float
) -> pd.DataFrame | None:
    """Normalize legacy compressed-time rtbench output.

    Current rtbench reports simulated time after undoing ``--delay-scale``.
    Older binaries reported compressed wall time. ``factor`` is the multiplier
    needed to put the latter into the current reporting domain.
    """
    if df is None or factor == 1.0:
        return df
    if not np.isfinite(factor) or factor <= 0:
        raise ValueError(f"rtbench time factor must be positive, got {factor}")

    d = df.copy()
    time_columns = {
        "throughput.csv": ["duration-ms", "cell-duration-ms"],
        "samples.csv": ["latency"],
        "deadlock.csv": ["latency-ms"],
        "deadlock-stats.csv": ["cell-duration-ms", "worker-drain-ms"],
    }
    rate_columns = {
        "throughput.csv": ["tx-per-sec"],
        "deadlock-stats.csv": ["tx-per-sec"],
    }
    for column in time_columns.get(name, []):
        if column in d.columns:
            d[column] *= factor
    for column in rate_columns.get(name, []):
        if column in d.columns:
            d[column] /= factor
    return d


def _ratio(b: float, a: float) -> float:
    return float("nan") if a == 0 else b / a


def _geomean(s: pd.Series) -> float:
    s = pd.Series(s).dropna()
    if (s == 0).any():
        return 0.0
    s = s[s > 0]
    if s.empty:
        return float("nan")
    return float(np.exp(np.log(s).mean()))


# Fallback only: for mixbench JSON that predates sequential sampling (no
# per-shape `converged` flag), a folded cell below this many committed
# transactions is too small to trust and its ratio is flagged `[low-sample]`.
# Current mixbench runs to a target CI instead, flagging `[unconverged]` when the
# time cap is hit first, so this floor is not consulted for fresh results.
LOW_SAMPLE_FLOOR = 1000

# Ratios within +/- this of 1.0 are called `~same` rather than better/worse, so
# run-to-run jitter is not read as a real move.
SAME_TOL = 0.02


def _verdict(ratio: float, lower_is_better: bool | None) -> str:
    """A direction-aware `=> better/WORSE/~same` tag for a ratio (b/a), or an
    empty string when the metric has no meaningful direction (or the ratio is
    NaN). `lower_is_better` encodes the metric's polarity: cost/latency/ops/
    retries improve as the ratio drops, throughput as it rises."""
    if lower_is_better is None or ratio != ratio:
        return ""
    if abs(ratio - 1.0) <= SAME_TOL:
        return " => ~same"
    good = (ratio < 1.0) if lower_is_better else (ratio > 1.0)
    return " => better" if good else " => WORSE"


def backend_ops_series(df: pd.DataFrame) -> pd.Series:
    """Total backend round-trips per row: the `backend-ops` column if present,
    else the sum of whatever per-class op columns exist (back-compat)."""
    if "backend-ops" in df.columns:
        return df["backend-ops"]
    present = [c for c in OP_COLS if c in df.columns]
    return df[present].sum(axis=1) if present else pd.Series(0, index=df.index)


def logical_tx_series(df: pd.DataFrame) -> pd.Series:
    """Logical benchmark operations, falling back to physical transaction calls
    for result files that predate benchmark-level in-doubt replay."""
    if "logical-tx" in df.columns:
        return df["logical-tx"]
    return df["num-tx"]


def with_run_identity(df: pd.DataFrame, legacy_identity: list[str]) -> pd.DataFrame:
    """Return a copy with an explicit one-based run column.

    Current rtbench output carries `run`. For a legacy aggregate, repeated rows
    of the same identity are numbered in encounter order. Sample files cannot
    be separated after pooling, so their legacy fallback is one run; the
    interleaving driver adds the run before it appends those files.
    """
    d = df.copy()
    if "run" in d.columns:
        return d
    if legacy_identity:
        d["run"] = d.groupby(legacy_identity, sort=False).cumcount() + 1
    else:
        d["run"] = 1
    return d


def paired_merge(
    a: pd.DataFrame, b: pd.DataFrame, keys: list[str], suffixes=("_a", "_b")
) -> pd.DataFrame:
    """Merge paired cells, rejecting missing runs instead of silently pooling."""
    a_keys = set(a[keys].itertuples(index=False, name=None))
    b_keys = set(b[keys].itertuples(index=False, name=None))
    if a_keys != b_keys:
        missing_a = sorted(b_keys - a_keys)
        missing_b = sorted(a_keys - b_keys)
        raise ValueError(
            f"unpaired benchmark cells: missing from a={missing_a}, "
            f"missing from b={missing_b}"
        )
    return a.merge(b, on=keys, suffixes=suffixes, validate="one_to_one")


def throughput_duration_column(df: pd.DataFrame) -> str:
    """Select the duration column and verify the modern shared-cell contract."""
    if "cell-duration-ms" not in df.columns:
        return "duration-ms"
    durations = df["cell-duration-ms"]
    if (~np.isfinite(durations) | (durations <= 0)).any():
        raise ValueError("throughput cells must have a positive finite duration")
    distinct = df.groupby(["run", "num-db"])["cell-duration-ms"].nunique(
        dropna=False
    )
    if (distinct != 1).any():
        cells = distinct[distinct != 1].index.tolist()
        raise ValueError(f"throughput rows disagree on the common cell clock: {cells}")
    return "cell-duration-ms"


def aggregate_throughput(df: pd.DataFrame) -> pd.DataFrame:
    """System throughput per run/cell/type from completions and one cell clock."""
    d = with_run_identity(df, ["num-db", "db", "tx-type"])
    duration_col = throughput_duration_column(d)
    grouped = d.groupby(["run", "num-db", "tx-type"], as_index=False).agg(
        count=("count", "sum"),
        cell_duration_ms=(duration_col, "max"),
    )
    grouped["total-tps"] = (
        grouped["count"] * 1000.0 / grouped["cell_duration_ms"].where(
            grouped["cell_duration_ms"] > 0
        )
    )
    return grouped


def throughput_fairness(df: pd.DataFrame) -> pd.DataFrame:
    """Per-Database completion-rate distribution and Jain fairness per cell."""
    d = with_run_identity(df, ["num-db", "db", "tx-type"])
    duration_col = throughput_duration_column(d)
    per_db = d.groupby(["run", "num-db", "db"], as_index=False).agg(
        count=("count", "sum"),
        cell_duration_ms=(duration_col, "max"),
    )
    per_db["db-tps"] = (
        per_db["count"] * 1000.0 / per_db["cell_duration_ms"].where(
            per_db["cell_duration_ms"] > 0
        )
    )
    rows = []
    for (run, num_db), group in per_db.groupby(["run", "num-db"]):
        rates = group["db-tps"]
        squared_sum = float((rates * rates).sum())
        jain = (
            float(rates.sum() ** 2 / (len(rates) * squared_sum))
            if squared_sum > 0
            else float("nan")
        )
        rows.append(
            {
                "run": run,
                "num-db": num_db,
                "db-tps-p10": rates.quantile(0.1),
                "db-tps-p50": rates.quantile(0.5),
                "db-tps-p90": rates.quantile(0.9),
                "jain": jain,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tables (each returns a merged frame with a `ratio` / `*-ratio` column)
# ---------------------------------------------------------------------------


def throughput_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Aggregate tx/s per paired run/cell/type using the common cell clock."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        g = aggregate_throughput(df)
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = paired_merge(
        agg(a),
        agg(b),
        ["run", "num-db", "tx-type", "concurrent"],
    )
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["total-tps_b"], r["total-tps_a"]), axis=1
    )
    return merged


def fairness_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        g = throughput_fairness(df)
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = paired_merge(agg(a), agg(b), ["run", "num-db", "concurrent"])
    merged["jain-ratio"] = merged.apply(
        lambda r: _ratio(r["jain_b"], r["jain_a"]), axis=1
    )
    return merged


def latency_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """p50/p90/p95 transaction latency (ms) per (concurrency, tx-type)."""
    pctiles = {"p50": 0.5, "p90": 0.9, "p95": 0.95}

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        df = with_run_identity(df, [])
        rows = []
        for (run, numdb, tp), grp in df.groupby(["run", "num-db", "tx-type"]):
            row = {"run": run, "num-db": numdb, "tx-type": tp}
            for name, q in pctiles.items():
                row[name] = grp["latency"].quantile(q)
            rows.append(row)
        out = pd.DataFrame(rows)
        out["concurrent"] = out["num-db"] * conc_per_db
        return out

    merged = paired_merge(
        agg(a),
        agg(b),
        ["run", "num-db", "tx-type", "concurrent"],
    )
    for p in pctiles:
        merged[f"{p}-ratio"] = merged.apply(
            lambda r, p=p: _ratio(r[f"{p}_b"], r[f"{p}_a"]), axis=1
        )
    return merged


def retries_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = with_run_identity(df, ["num-db", "db"])
        d["logical-tx"] = logical_tx_series(d)
        g = (
            d.groupby(["run", "num-db"], as_index=False)
            .agg({"num-retries": "sum", "logical-tx": "sum"})
        )
        g["retries-per-tx"] = g["num-retries"] / g["logical-tx"].where(
            g["logical-tx"] > 0
        )
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = paired_merge(agg(a), agg(b), ["run", "num-db", "concurrent"])
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["retries-per-tx_b"], r["retries-per-tx_a"]), axis=1
    )
    return merged


def backend_ops_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Backend round-trips per committed transaction per concurrency step."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        d = with_run_identity(df, ["num-db", "db"])
        d["backend-ops"] = backend_ops_series(d)
        d["logical-tx"] = logical_tx_series(d)
        g = (
            d.groupby(["run", "num-db"])
            .agg({"backend-ops": "sum", "logical-tx": "sum"})
            .reset_index()
        )
        g["ops-per-tx"] = g["backend-ops"] / g["logical-tx"].where(
            g["logical-tx"] > 0
        )
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    merged = paired_merge(agg(a), agg(b), ["run", "num-db", "concurrent"])
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["ops-per-tx_b"], r["ops-per-tx_a"]), axis=1
    )
    return merged


def diagnostic_metrics_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Opt-in component metrics normalized by logical benchmark operations."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        df = with_run_identity(df, [])
        g = (
            df.groupby(["run", "num-db", "component", "metric"])
            .agg({"value": "sum", "logical-tx": "sum"})
            .reset_index()
        )
        g["per-tx"] = g["value"] / g["logical-tx"].where(g["logical-tx"] > 0)
        g["concurrent"] = g["num-db"] * conc_per_db
        return g

    a_agg, b_agg = agg(a), agg(b)
    metric_keys = ["component", "metric"]
    common_metrics = (
        a_agg[metric_keys]
        .drop_duplicates()
        .merge(b_agg[metric_keys].drop_duplicates(), on=metric_keys)
    )
    a_agg = a_agg.merge(common_metrics, on=metric_keys)
    b_agg = b_agg.merge(common_metrics, on=metric_keys)
    merged = paired_merge(
        a_agg,
        b_agg,
        ["run", "num-db", "component", "metric", "concurrent"],
    )
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["per-tx_b"], r["per-tx_a"]), axis=1
    )
    return merged


def diagnostic_batch_table(a: pd.DataFrame, b: pd.DataFrame, conc_per_db: int):
    """Coordinator submissions per round, a direction-neutral batching signal."""

    def agg(df: pd.DataFrame) -> pd.DataFrame:
        df = with_run_identity(df, [])
        d = df[
            (df["component"] == "coordinator")
            & (df["metric"].isin(["submissions", "rounds"]))
        ]
        if d.empty:
            return pd.DataFrame(
                columns=["run", "num-db", "concurrent", "batch-factor"]
            )
        g = (
            d.groupby(["run", "num-db", "metric"])["value"]
            .sum()
            .unstack(fill_value=0)
        )
        if "submissions" not in g or "rounds" not in g:
            return pd.DataFrame(
                columns=["run", "num-db", "concurrent", "batch-factor"]
            )
        g["batch-factor"] = g["submissions"] / g["rounds"].where(g["rounds"] > 0)
        g = g.reset_index()
        g["concurrent"] = g["num-db"] * conc_per_db
        return g[["run", "num-db", "concurrent", "batch-factor"]]

    merged = paired_merge(agg(a), agg(b), ["run", "num-db", "concurrent"])
    merged["ratio"] = merged.apply(
        lambda r: _ratio(r["batch-factor_b"], r["batch-factor_a"]), axis=1
    )
    return merged


def diagnostic_role_totals(
    table: pd.DataFrame, metrics: list[str]
) -> pd.DataFrame:
    """Fold selected backend metrics by physical object role."""
    selected = table[
        table["component"].str.startswith("backend.")
        & table["metric"].isin(metrics)
    ]
    if selected.empty:
        return pd.DataFrame()
    totals = (
        selected.groupby(["run", "concurrent", "component"])
        .agg({"per-tx_a": "sum", "per-tx_b": "sum"})
        .reset_index()
    )
    totals["ratio"] = totals.apply(
        lambda r: _ratio(r["per-tx_b"], r["per-tx_a"]), axis=1
    )
    return totals


def inline_pressure_table(a: pd.DataFrame, b: pd.DataFrame) -> pd.DataFrame:
    """Pair the fixed phases of the inline-pressure scenario by run."""

    def select(df: pd.DataFrame) -> pd.DataFrame:
        d = with_run_identity(df, ["phase"])
        # The first version of this scenario reported shutdown separately.
        # Current artifacts fold it into total, so ignore it when comparing
        # across the schema boundary.
        d = d[d["phase"] != "shutdown"].copy()
        logical_tx = d["logical-tx"].where(d["logical-tx"] > 0)
        direct_candidates = d["direct-candidates"].where(
            d["direct-candidates"] > 0
        )
        d["direct-land-rate"] = d["direct-landed"] / direct_candidates
        d["lock-calls-per-tx"] = d["lock-calls"] / logical_tx
        d["backend-ops-per-tx"] = d["backend-ops"] / logical_tx
        d["write-bytes-per-tx"] = d["write-bytes"] / logical_tx
        return d

    merged = paired_merge(select(a), select(b), ["run", "phase"])
    for metric in [
        "tx-per-sec",
        "p50-ms",
        "p90-ms",
        "lock-calls-per-tx",
        "backend-ops-per-tx",
        "write-bytes-per-tx",
    ]:
        merged[f"{metric}-ratio"] = merged.apply(
            lambda r, metric=metric: _ratio(r[f"{metric}_b"], r[f"{metric}_a"]),
            axis=1,
        )
    return merged


def deadlock_table(a: pd.DataFrame, b: pd.DataFrame):
    def agg(df: pd.DataFrame) -> pd.DataFrame:
        df = with_run_identity(df, [])
        d = df[df["overlap-pct"] == 100]
        if d.empty:
            d = df
        g = (
            d.groupby(["run", "num-keys"])["latency-ms"]
            .quantile([0.5, 0.9])
            .unstack()
            .reset_index()
        )
        g.columns = ["run", "num-keys", "p50", "p90"]
        return g

    merged = paired_merge(agg(a), agg(b), ["run", "num-keys"])
    for pct in ("p50", "p90"):
        merged[f"{pct}-ratio"] = merged.apply(
            lambda r, p=pct: _ratio(r[f"{p}_b"], r[f"{p}_a"]), axis=1
        )
    return merged


def deadlock_stats_table(a: pd.DataFrame, b: pd.DataFrame):
    def select(df: pd.DataFrame) -> pd.DataFrame:
        d = with_run_identity(df, [])
        d = d[d["overlap-pct"] == 100].copy()
        if d.empty:
            d = with_run_identity(df, [])
        candidates_col = (
            "direct-candidates"
            if "direct-candidates" in d.columns
            else "direct-attempts"
        )
        landed_col = (
            "direct-landed" if "direct-landed" in d.columns else "direct-commits"
        )
        d["tx-per-sec"] = (
            d["count"] * 1000.0 / d["cell-duration-ms"].where(
                d["cell-duration-ms"] > 0
            )
        )
        d["retries-per-tx"] = d["num-retries"] / d["count"].where(d["count"] > 0)
        d["direct-candidates-per-tx"] = (
            d[candidates_col] / d["count"].where(d["count"] > 0)
        )
        d["direct-land-rate"] = d[landed_col] / d[candidates_col].where(
            d[candidates_col] > 0
        )
        return d[
            [
                "run",
                "num-keys",
                "tx-per-sec",
                "retries-per-tx",
                "direct-candidates-per-tx",
                "direct-land-rate",
                "worker-drain-ms",
            ]
        ]

    merged = paired_merge(select(a), select(b), ["run", "num-keys"])
    for metric in [
        "tx-per-sec",
        "retries-per-tx",
        "direct-candidates-per-tx",
        "direct-land-rate",
        "worker-drain-ms",
    ]:
        merged[f"{metric}-ratio"] = merged.apply(
            lambda r, metric=metric: _ratio(r[f"{metric}_b"], r[f"{metric}_a"]),
            axis=1,
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
                    "committed_a": x.get("committed", float("nan")),
                    "committed_b": y.get("committed", float("nan")),
                    # mixbench sequential sampling: True once the shape's
                    # throughput CI met --target-ci. Absent (None) for legacy
                    # JSON, in which case the digest falls back to a sample floor.
                    "converged_a": x.get("converged"),
                    "converged_b": y.get("converged"),
                    "relCi_b": y.get("relCi", float("nan")),
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


def _folded_converged(grp: pd.DataFrame) -> pd.Series | None:
    """A cell's combined convergence (both sides reached `--target-ci`) for a
    folded group of mixbench rows, or `None` when the JSON predates sequential
    sampling (so the digest falls back to the sample-count floor). A missing
    per-side flag is treated as converged so legacy-vs-new mixes never spuriously
    report `[unconverged]`."""
    a, b = grp["converged_a"], grp["converged_b"]
    if a.isna().all() and b.isna().all():
        return None
    return a.fillna(True).astype(bool) & b.fillna(True).astype(bool)


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


def summarize(
    name: str,
    ratios: pd.Series,
    *,
    lower_is_better: bool | None = None,
    samples: pd.Series | None = None,
    converged: pd.Series | None = None,
    noisy: bool = False,
) -> str:
    """One digest line for a set of ratios.

    `lower_is_better` adds a direction-aware verdict. `converged` (per-cell
    booleans from mixbench's sequential sampling) flags `[unconverged]` when any
    folded cell hit its time cap before reaching the target confidence interval,
    so its throughput is only indicative. When `converged` is absent, `samples`
    (per-cell committed-transaction counts) is the fallback reliability signal,
    flagging `[low-sample]` below [`LOW_SAMPLE_FLOOR`]. `noisy` marks metrics that
    are run-to-run variable rather than deterministic. A single ratio is reported
    as one value — never as a fake `min=median=max` distribution."""
    r = pd.Series(ratios).dropna()
    if r.empty:
        return f"{name}: no data"

    tag = " [noisy]" if noisy else ""
    n_note = ""
    if samples is not None:
        s = pd.Series(samples).dropna()
        if not s.empty:
            n_note = f" n_min={int(s.min())}"
    if converged is not None:
        c = pd.Series(converged).dropna()
        if not c.empty and not bool(c.all()):
            tag += " [unconverged]"
    elif samples is not None:
        s = pd.Series(samples).dropna()
        if not s.empty and s.min() < LOW_SAMPLE_FLOOR:
            tag += " [low-sample]"

    if len(r) == 1:
        v = float(r.iloc[0])
        body = f"ratio b/a={v:.2f} (1 point)"
        verdict = _verdict(v, lower_is_better)
    else:
        body = (
            f"ratio b/a min={r.min():.2f} median={r.median():.2f} "
            f"max={r.max():.2f} (geomean={_geomean(r):.2f}, n={len(r)})"
        )
        verdict = _verdict(r.median(), lower_is_better)
    return f"{name}{tag}: {body}{n_note}{verdict}"


def append_summary(path: Path, title: str, summaries: list[str]) -> None:
    """Append a small markdown section for this comparison to ``path``.

    The shell driver points every comparison at the same file so the result is
    one compact, trackable digest per run. Each line carries its own polarity
    verdict (`=> better/WORSE/~same`) and, where relevant, a sample-size note and
    a `[noisy]`/`[unconverged]` tag; the autoresearch section is deterministic,
    mixbench cells run to a target CI (flagged `[unconverged]` if the time cap is
    hit first), and the deadlock section is indicative only."""
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
        d = aggregate_throughput(df)
        d["concurrent"] = d["num-db"] * conc_per_db
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
        d = with_run_identity(df, ["num-db", "db"])
        d["logical-tx"] = logical_tx_series(d)
        d = (
            d.groupby(["run", "num-db"], as_index=False)
            .agg({"num-retries": "sum", "logical-tx": "sum"})
        )
        d["concurrent"] = d["num-db"] * conc_per_db
        d["retries-per-tx"] = d["num-retries"] / d["logical-tx"].where(
            d["logical-tx"] > 0
        )
        d["source"] = src
        frames.append(d)
    return pd.concat(frames, ignore_index=True)


def _tidy_deadlock(a, b, la, lb):
    frames = []
    for src, df in ((la, a), (lb, b)):
        d = with_run_identity(df, [])
        d = d[d["overlap-pct"] == 100].copy()
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
    parser.add_argument(
        "--rtbench-time-factor-a",
        type=float,
        default=1.0,
        help="multiply legacy rtbench timings from --a by this factor",
    )
    parser.add_argument(
        "--rtbench-time-factor-b",
        type=float,
        default=1.0,
        help="multiply legacy rtbench timings from --b by this factor",
    )
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

    a_tp = normalize_rtbench_time(
        read_csv(args.a, "throughput.csv"),
        "throughput.csv",
        args.rtbench_time_factor_a,
    )
    b_tp = normalize_rtbench_time(
        read_csv(args.b, "throughput.csv"),
        "throughput.csv",
        args.rtbench_time_factor_b,
    )
    if a_tp is not None and b_tp is not None:
        tbl = throughput_table(a_tp, b_tp, cpd)
        cols = [
            "run",
            "concurrent",
            "tx-type",
            "total-tps_a",
            "total-tps_b",
            "ratio",
        ]
        print_table(f"Aggregate throughput (total tx/s, {lb}/{la})", tbl[cols])
        for tx_type, grp in tbl.groupby("tx-type"):
            summaries.append(
                summarize(f"throughput[{tx_type}]", grp["ratio"], lower_is_better=False)
            )
        fair = fairness_table(a_tp, b_tp, cpd)
        fair_cols = [
            "run",
            "concurrent",
            "db-tps-p10_a",
            "db-tps-p50_a",
            "db-tps-p90_a",
            "jain_a",
            "db-tps-p10_b",
            "db-tps-p50_b",
            "db-tps-p90_b",
            "jain_b",
            "jain-ratio",
        ]
        print_table(
            f"Per-Database throughput fairness ({lb}/{la})",
            fair[fair_cols],
        )
        summaries.append(
            summarize(
                "throughput-fairness[jain]",
                fair["jain-ratio"],
                lower_is_better=False,
            )
        )

    a_la = normalize_rtbench_time(
        read_csv(args.a, "samples.csv"),
        "samples.csv",
        args.rtbench_time_factor_a,
    )
    b_la = normalize_rtbench_time(
        read_csv(args.b, "samples.csv"),
        "samples.csv",
        args.rtbench_time_factor_b,
    )
    if a_la is not None and b_la is not None:
        tbl = latency_table(a_la, b_la, cpd)
        cols = [
            "run",
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
            summaries.append(
                summarize(
                    f"latency-p50[{tx_type}]", grp["p50-ratio"], lower_is_better=True
                )
            )

    a_st, b_st = read_csv(args.a, "stats.csv"), read_csv(args.b, "stats.csv")
    if a_st is not None and b_st is not None:
        tbl = retries_table(a_st, b_st, cpd)
        cols = [
            "run",
            "concurrent",
            "retries-per-tx_a",
            "retries-per-tx_b",
            "ratio",
        ]
        print_table(f"Retries per transaction ({lb}/{la})", tbl[cols])
        summaries.append(summarize("retries", tbl["ratio"], lower_is_better=True))

        tbl = backend_ops_table(a_st, b_st, cpd)
        cols = ["run", "concurrent", "ops-per-tx_a", "ops-per-tx_b", "ratio"]
        print_table(f"Backend round-trips per transaction ({lb}/{la})", tbl[cols])
        summaries.append(
            summarize("backend-ops/tx", tbl["ratio"], lower_is_better=True)
        )

    a_diag = read_csv(args.a / "diagnostics", "metrics.csv")
    b_diag = read_csv(args.b / "diagnostics", "metrics.csv")
    if a_diag is not None and b_diag is not None:
        tbl = diagnostic_metrics_table(a_diag, b_diag, cpd)
        cols = [
            "run",
            "concurrent",
            "component",
            "metric",
            "per-tx_a",
            "per-tx_b",
            "ratio",
        ]
        print_table(f"Diagnostic metrics per transaction ({lb}/{la})", tbl[cols])

        role_totals = diagnostic_role_totals(tbl, ["reads", "writes", "lists"])
        if not role_totals.empty:
            for component, group in role_totals.groupby("component"):
                summaries.append(
                    summarize(
                        f"diag-ops/tx[{component}]",
                        group["ratio"],
                        lower_is_better=True,
                    )
                )
        role_totals = diagnostic_role_totals(
            tbl, ["read-bytes", "write-bytes"]
        )
        if not role_totals.empty:
            for component, group in role_totals.groupby("component"):
                summaries.append(
                    summarize(
                        f"diag-bytes/tx[{component}]",
                        group["ratio"],
                        lower_is_better=True,
                    )
                )

        protocol = tbl[~tbl["component"].str.startswith("backend.")]
        for (component, metric), group in protocol.groupby(["component", "metric"]):
            direction = (
                None
                if component == "splitter"
                or (
                    component == "direct_commit"
                    and metric in ["candidates", "landed"]
                )
                else True
            )
            summaries.append(
                summarize(
                    f"diag-{component}/tx[{metric}]",
                    group["ratio"],
                    lower_is_better=direction,
                )
            )

        batch = diagnostic_batch_table(a_diag, b_diag, cpd)
        print_table(
            f"Coordinator batching factor ({lb}/{la}; direction-neutral)",
            batch,
        )
        if not batch.empty:
            summaries.append(
                summarize("diag-coordinator[batch-factor]", batch["ratio"])
            )

    a_ip = read_csv(args.a, "inline-pressure.csv")
    b_ip = read_csv(args.b, "inline-pressure.csv")
    if a_ip is not None and b_ip is not None:
        tbl = inline_pressure_table(a_ip, b_ip)
        cols = [
            "run",
            "phase",
            "tx-per-sec_a",
            "tx-per-sec_b",
            "tx-per-sec-ratio",
            "p50-ms-ratio",
            "direct-land-rate_a",
            "direct-land-rate_b",
            "lock-calls-per-tx_a",
            "lock-calls-per-tx_b",
            "backend-ops-per-tx_a",
            "backend-ops-per-tx_b",
            "backend-ops-per-tx-ratio",
            "write-bytes-per-tx_a",
            "write-bytes-per-tx_b",
            "write-bytes-per-tx-ratio",
            "split-completed_a",
            "split-completed_b",
            "pressure-completed_a",
            "pressure-completed_b",
        ]
        print_table(f"Inline-pressure phases ({lb}/{la})", tbl[cols])
        recovery = tbl[tbl["phase"] == "recovery"]
        total = tbl[tbl["phase"] == "total"]
        for metric, lower_is_better in [
            ("tx-per-sec", False),
            ("p50-ms", True),
            ("p90-ms", True),
            ("lock-calls-per-tx", True),
            ("backend-ops-per-tx", True),
            ("write-bytes-per-tx", True),
        ]:
            summaries.append(
                summarize(
                    f"inline-recovery-{metric}",
                    recovery[f"{metric}-ratio"],
                    lower_is_better=lower_is_better,
                )
            )
        for metric in ["backend-ops-per-tx", "write-bytes-per-tx"]:
            summaries.append(
                summarize(
                    f"inline-total-{metric}",
                    total[f"{metric}-ratio"],
                    lower_is_better=True,
                )
            )
        if not recovery.empty:
            summaries.append(
                "inline-recovery-direct-land-rate: "
                f"{la}={recovery['direct-land-rate_a'].median():.3f} "
                f"{lb}={recovery['direct-land-rate_b'].median():.3f}"
            )
        if not total.empty:
            summaries.append(
                "inline-total-completed-splits: "
                f"{la}={total['split-completed_a'].median():.1f} "
                f"{lb}={total['split-completed_b'].median():.1f}"
            )

    a_dl = normalize_rtbench_time(
        read_csv(args.a, "deadlock.csv"),
        "deadlock.csv",
        args.rtbench_time_factor_a,
    )
    b_dl = normalize_rtbench_time(
        read_csv(args.b, "deadlock.csv"),
        "deadlock.csv",
        args.rtbench_time_factor_b,
    )
    if a_dl is not None and b_dl is not None:
        tbl = deadlock_table(a_dl, b_dl)
        print_table(f"Deadlock latency at 100% overlap (ms, {lb}/{la})", tbl)
        summaries.append(
            summarize(
                "deadlock-p50", tbl["p50-ratio"], lower_is_better=True, noisy=True
            )
        )
        summaries.append(
            summarize(
                "deadlock-p90", tbl["p90-ratio"], lower_is_better=True, noisy=True
            )
        )

    a_ds = normalize_rtbench_time(
        read_csv(args.a, "deadlock-stats.csv"),
        "deadlock-stats.csv",
        args.rtbench_time_factor_a,
    )
    b_ds = normalize_rtbench_time(
        read_csv(args.b, "deadlock-stats.csv"),
        "deadlock-stats.csv",
        args.rtbench_time_factor_b,
    )
    if a_ds is not None and b_ds is not None:
        tbl = deadlock_stats_table(a_ds, b_ds)
        cols = [
            "run",
            "num-keys",
            "tx-per-sec_a",
            "tx-per-sec_b",
            "tx-per-sec-ratio",
            "retries-per-tx_a",
            "retries-per-tx_b",
            "retries-per-tx-ratio",
            "direct-candidates-per-tx_a",
            "direct-candidates-per-tx_b",
            "direct-land-rate_a",
            "direct-land-rate_b",
            "worker-drain-ms_a",
            "worker-drain-ms_b",
        ]
        print_table(f"Deadlock completion and protocol outcomes ({lb}/{la})", tbl[cols])
        for metric, lower_is_better in [
            ("tx-per-sec", False),
            ("retries-per-tx", True),
            ("direct-candidates-per-tx", True),
            ("direct-land-rate", False),
            ("worker-drain-ms", True),
        ]:
            summaries.append(
                summarize(
                    f"deadlock-{metric}",
                    tbl[f"{metric}-ratio"],
                    lower_is_better=lower_is_better,
                    noisy=True,
                )
            )

    a_sc, b_sc = read_json(args.a, "score.json"), read_json(args.b, "score.json")
    if a_sc is not None and b_sc is not None:
        sa, sb = a_sc.get("score"), b_sc.get("score")
        if sa is not None and sb is not None:
            print("\n## Autoresearch primary score (cost/tx geomean, lower = better)\n")
            score_ratio = _ratio(sb, sa)
            print(f"{la}={sa:.2f}  {lb}={sb:.2f}  ratio({lb}/{la})={score_ratio:.3f}")
            # Deterministic single-client backend-ops-per-tx cost: the direction
            # is spelled out because a *lower* score is better (unlike throughput),
            # which is the axis most easily misread.
            summaries.append(
                "autoresearch-score (cost/tx geomean, lower=better) [deterministic]: "
                f"{la}={sa:.2f} {lb}={sb:.2f} ratio b/a={score_ratio:.3f}"
                f"{_verdict(score_ratio, True)}"
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
            summaries.append(
                summarize(
                    "autoresearch-cost/tx", tbl["cost-ratio"], lower_is_better=True
                )
            )
            summaries.append(
                summarize("autoresearch-ops/tx", tbl["ops-ratio"], lower_is_better=True)
            )
            # Per-workload cost so a big localized change (e.g. singleRMW) is not
            # diluted by the geomean; this is the deterministic signal that most
            # cleanly attributes a single-RW / read / batch effect.
            for _, row in tbl.sort_values("cost-ratio").iterrows():
                summaries.append(
                    summarize(
                        f"autoresearch-cost/tx[{row['workload']}]",
                        pd.Series([row["cost-ratio"]]),
                        lower_is_better=True,
                    )
                )

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
            # mixbench's sequential sampling runs each cell until its throughput
            # CI meets --target-ci, so a converged tps ratio is significant; a cell
            # that hit the time cap first is flagged [unconverged] (see
            # `_folded_converged`).
            for shape, grp in tbl.groupby("shape"):
                summaries.append(
                    summarize(
                        f"mix-tps[{shape}]",
                        grp["tps-ratio"],
                        lower_is_better=False,
                        samples=grp["committed_b"],
                        converged=_folded_converged(grp),
                    )
                )
            # ops/tx + retries/tx are per-shape only where a shape owns its DBs
            # (the `per-shape` topology); keep the mode split so the hi-contention
            # dedup signal is not washed out.
            ops = tbl.dropna(subset=["ops-ratio"])
            for (mode, shape), grp in ops.groupby(["mode", "shape"]):
                summaries.append(
                    summarize(
                        f"mix-ops/tx[{mode}/{shape}]",
                        grp["ops-ratio"],
                        lower_is_better=True,
                        samples=grp["committed_b"],
                        converged=_folded_converged(grp),
                    )
                )
            for mode, grp in ops.groupby("mode"):
                summaries.append(
                    summarize(
                        f"mix-retries/tx[{mode}]",
                        grp["retries-ratio"],
                        lower_is_better=True,
                        converged=_folded_converged(grp),
                    )
                )
        agg = mixbench_aggregate_table(a_mx, b_mx)
        if not agg.empty:
            print_table(f"mixbench shared-DB aggregate ops/tx ({lb}/{la})", agg)
            for mode, grp in agg.groupby("mode"):
                summaries.append(
                    summarize(
                        f"mix-agg-ops/tx[{mode}]",
                        grp["ops-ratio"],
                        lower_is_better=True,
                    )
                )

    print(
        "\n## Summary (ratio = b/a; throughput >1 good, latency/ops/cost <1 good; "
        "=> tag reads the right direction per metric; [noisy] = run-to-run "
        "variable, [unconverged] = mixbench hit its time cap before reaching "
        "--target-ci so read as indicative, [low-sample] = legacy fallback)\n"
    )
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
