#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Render perfbench topology policy results (ADR-074) as plots and one HTML report.

Inputs are perfbench `topology` and `mixed` JSON files, as `PATH` or
`PATH=POLICY,POLICY`, which keeps only those policies and the baselines. The
delay model is not in the JSON, so each file name must contain `-s3-` or
`-gcs-`. Each ratio divides by the baseline of the same file and run.

The throughput of a mixed cell is the geometric mean of its shapes, so that a
shape that starves counts as much as a fast one.
"""

from __future__ import annotations

import argparse
import base64
import html
import io
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

WORKLOADS = ("single", "hot", "adjacent", "random", "scan")
SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")
TOPOLOGY_KEYS = ["delays", "workload", "leaf", "databases"]
MIXED_KEYS = ["delays", "mode", "affinity", "databases"]
REPLICA_KEYS = ["source", "run"]
TOPOLOGY_COLUMNS = [
    *TOPOLOGY_KEYS,
    *REPLICA_KEYS,
    "policy",
    "tx_per_sec",
    "vs_baseline",
    "p50_ms",
    "adapt_splits",
    "adapt_merges",
    "final_leaves",
    "ops_per_tx",
]
MIXED_COLUMNS = [
    *MIXED_KEYS,
    *REPLICA_KEYS,
    "policy",
    "tx_per_sec",
    "vs_baseline",
    *(f"{name}_tx_per_sec" for name in SHAPES),
    "ops_per_tx",
    "warmup_splits",
    "warmup_merges",
    "measured_splits",
    "measured_merges",
]
STYLE = (
    "body{font-family:sans-serif;max-width:1500px;margin:auto}"
    "img{max-width:100%;display:block;margin:1em 0}"
    "table{border-collapse:collapse;font-size:12px}"
    "td,th{padding:2px 6px;text-align:right;border-bottom:1px solid #ddd}"
)


def delay_model(path: Path) -> str:
    match = re.search(r"-(s3|gcs)-", path.name)
    if match is None:
        raise ValueError(f"{path}: the file name must contain -s3- or -gcs-")
    return match.group(1)


def runs(data: dict[str, Any]) -> list[dict[str, Any]]:
    return data["runs"] if "runs" in data else [data]


def topology_rows(path: Path, data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for index, run in enumerate(runs(data)):
        for cell in run["cells"]:
            per_tx = cell["perTx"]
            rows.append(
                {
                    "delays": delay_model(path),
                    "source": path.name,
                    "run": index,
                    "workload": cell["workload"],
                    "leaf": cell["leafMaxEntries"],
                    "databases": cell["databases"],
                    "policy": cell["policy"],
                    "tx_per_sec": cell["txPerSec"],
                    "p50_ms": cell["p50Ms"],
                    "adapt_splits": cell["adaptSplits"],
                    "adapt_merges": cell["adaptMerges"],
                    "final_leaves": cell.get("finalLeaves"),
                    "ops_per_tx": per_tx["backendOps"],
                }
            )
    return rows


def mixed_rows(path: Path, data: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for index, run in enumerate(runs(data)):
        for cell in run["cells"]:
            shapes = {shape["shape"]: shape for shape in cell["shapes"]}
            restructure = cell["restructure"]
            logs = [math.log(max(shape["txPerSec"], 1e-3)) for shape in shapes.values()]
            row = {
                "delays": delay_model(path),
                "source": path.name,
                "run": index,
                "mode": cell["mode"],
                "affinity": cell["affinityPct"],
                "databases": cell["databases"],
                "policy": cell["policy"],
                "tx_per_sec": math.exp(sum(logs) / len(logs)),
                "ops_per_tx": cell["aggregateOps"]["totalOpsPerTx"],
                "warmup_splits": restructure["warmupSplits"],
                "warmup_merges": restructure["warmupMerges"],
                "measured_splits": restructure["measuredSplits"],
                "measured_merges": restructure["measuredMerges"],
            }
            for name in SHAPES:
                shape = shapes.get(name)
                row[f"{name}_tx_per_sec"] = shape["txPerSec"] if shape else 0.0
                row[f"{name}_p50_ms"] = shape["p50Ms"] if shape else 0.0
            rows.append(row)
    return rows


def load(inputs: list[str], baselines: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    topology: list[dict[str, Any]] = []
    mixed: list[dict[str, Any]] = []
    for spec in inputs:
        name, _, selected = spec.partition("=")
        path = Path(name)
        data = json.loads(path.read_text())
        is_topology = data.get("scenario") == "topology"
        rows = topology_rows(path, data) if is_topology else mixed_rows(path, data)
        if selected:
            keep = set(selected.split(",")) | baselines
            rows = [row for row in rows if row["policy"] in keep]
        (topology if is_topology else mixed).extend(rows)
    return pd.DataFrame(topology), pd.DataFrame(mixed)


def table_html(frame: pd.DataFrame) -> str:
    return frame.to_html(
        index=False, float_format=lambda value: f"{value:.2f}", border=0
    )


def geomean(values: pd.Series) -> float:
    return math.exp(values.map(math.log).mean())


def relative_to_baseline(
    frame: pd.DataFrame, keys: list[str], baseline: str
) -> pd.DataFrame:
    """Adds the throughput of each row divided by the baseline's in the same
    file and run, and drops the rows that have no baseline."""
    keys = keys + REPLICA_KEYS
    base = frame[frame["policy"] == baseline].set_index(keys)["tx_per_sec"]
    joined = frame.join(base.rename("baseline_tx_per_sec"), on=keys)
    joined["vs_baseline"] = joined["tx_per_sec"] / joined["baseline_tx_per_sec"]
    return joined.dropna(subset=["vs_baseline"])


@dataclass
class Report:
    """Collects the sections of the HTML report."""

    topology_baseline: str
    mixed_baseline: str
    image_dir: Path | None
    sections: list[str] = field(default_factory=list)
    images: int = 0

    def summary(self, topology: pd.DataFrame, mixed: pd.DataFrame) -> None:
        """Adds the geometric mean and the worst cell of each policy against
        its baseline, for each scenario and delay model."""
        parts = []
        if not topology.empty:
            data = relative_to_baseline(topology, TOPOLOGY_KEYS, self.topology_baseline)
            parts.append(self._per_cell(data, TOPOLOGY_KEYS, "topology"))
        if not mixed.empty:
            data = relative_to_baseline(mixed, MIXED_KEYS, self.mixed_baseline)
            for mode in sorted(set(data["mode"])):
                cells = data[data["mode"] == mode]
                parts.append(self._per_cell(cells, MIXED_KEYS, f"mixed {mode}"))
        cells = pd.concat(parts)
        grouped = cells.groupby(["scenario", "delays", "policy"])
        table = pd.DataFrame(
            {
                "geomean": grouped["vs_baseline"].agg(geomean),
                "worst cell": grouped["vs_baseline"].min(),
                "best cell": grouped["vs_baseline"].max(),
                "cells": grouped["vs_baseline"].size(),
                "replicas": grouped["replicas"].sum(),
            }
        ).reset_index()
        grid = sns.catplot(
            data=table,
            kind="bar",
            x="policy",
            y="geomean",
            order=self._policy_order(table),
            col="scenario",
            row="delays",
            height=3.2,
            aspect=1.5,
            sharey=False,
        )
        for axis in grid.axes.flat:
            axis.axhline(1.0, color="black", linewidth=0.8)
            axis.tick_params(axis="x", rotation=40)
        grid.set_axis_labels("policy", "geomean vs baseline")
        self.sections.append(
            "<h2>Summary</h2><p>Each cell is the geometric mean over its "
            "replicas (files and runs) of the policy's throughput divided by "
            "the baseline of the same replica. The baseline is "
            f"<code>{html.escape(self.topology_baseline)}</code> for topology "
            f"and <code>{html.escape(self.mixed_baseline)}</code> for mixed.</p>"
        )
        self._add_figure(grid.figure)
        self.sections.append(table_html(table))

    def topology(self, frame: pd.DataFrame) -> None:
        frame = relative_to_baseline(frame, TOPOLOGY_KEYS, self.topology_baseline)
        for delays in sorted(set(frame["delays"])):
            data = frame[frame["delays"] == delays]
            self.sections.append(
                f"<h2>Topology workloads, {html.escape(delays.upper())}</h2>"
            )
            self._topology_throughput(data, delays)
            self._topology_heatmap(
                data,
                "vs_baseline",
                f"{delays.upper()}: throughput relative to {self.topology_baseline}",
            )
            self._topology_heatmap(
                data.assign(changes=data["adapt_splits"] + data["adapt_merges"]),
                "changes",
                f"{delays.upper()}: splits and merges while adapting",
            )
        self.sections.append(
            "<h2>Topology cells</h2>" + table_html(frame[TOPOLOGY_COLUMNS])
        )

    def mixed(self, frame: pd.DataFrame) -> None:
        frame = relative_to_baseline(frame, MIXED_KEYS, self.mixed_baseline)
        for delays in sorted(set(frame["delays"])):
            data = frame[frame["delays"] == delays]
            self.sections.append(
                f"<h2>Mixed workload, {html.escape(delays.upper())}</h2>"
                "<p>Throughput is the geometric mean of the four shapes. "
                "Affinity 0% means that every database picks from all "
                "collections; 100% means that each uses only its own.</p>"
            )
            self._mixed_lines(data, delays, "affinity", "databases", "vs_baseline")
            self._mixed_lines(data, delays, "databases", "affinity", "vs_baseline")
            self._mixed_shapes(data, delays)
        self.sections.append("<h2>Mixed cells</h2>" + table_html(frame[MIXED_COLUMNS]))

    def html(self) -> str:
        return (
            f"<!doctype html><html><head><meta charset='utf-8'><style>{STYLE}</style>"
            "<title>Topology policy experiments</title></head><body>"
            "<h1>Topology policy experiments (ADR-074)</h1>"
            + "".join(self.sections)
            + "</body></html>"
        )

    def _per_cell(
        self, data: pd.DataFrame, keys: list[str], scenario: str
    ) -> pd.DataFrame:
        grouped = data.groupby([*keys, "policy"])["vs_baseline"]
        cells = grouped.agg(geomean).reset_index()
        cells["replicas"] = grouped.size().to_numpy()
        return cells.assign(scenario=scenario)

    def _policy_order(self, frame: pd.DataFrame) -> list[str]:
        baselines = (self.topology_baseline, self.mixed_baseline)
        return sorted(
            set(frame["policy"]),
            key=lambda policy: (policy not in baselines, policy),
        )

    def _add_figure(self, figure: plt.Figure) -> None:
        buffer = io.BytesIO()
        figure.savefig(buffer, format="png", dpi=110, bbox_inches="tight")
        plt.close(figure)
        if self.image_dir is not None:
            (self.image_dir / f"{self.images:02}.png").write_bytes(buffer.getvalue())
        self.images += 1
        encoded = base64.b64encode(buffer.getvalue()).decode()
        self.sections.append(f'<img src="data:image/png;base64,{encoded}">')

    def _topology_throughput(self, data: pd.DataFrame, delays: str) -> None:
        data = data.assign(
            cell="L" + data["leaf"].astype(str) + " db" + data["databases"].astype(str)
        )
        grid = sns.catplot(
            data=data,
            kind="bar",
            x="cell",
            y="tx_per_sec",
            hue="policy",
            hue_order=self._policy_order(data),
            col="workload",
            col_order=[w for w in WORKLOADS if w in set(data["workload"])],
            col_wrap=3,
            sharey=False,
            height=3.2,
            aspect=1.3,
        )
        grid.set_axis_labels("initial leaf size, databases", "tx/s")
        grid.figure.suptitle(
            f"Topology workloads, {delays.upper()} delays: throughput "
            "(bars: mean over replicas, lines: 95% CI)",
            y=1.02,
        )
        self._add_figure(grid.figure)

    def _topology_heatmap(self, data: pd.DataFrame, value: str, title: str) -> None:
        data = data.assign(
            cell=data["workload"]
            + " L"
            + data["leaf"].astype(str)
            + " db"
            + data["databases"].astype(str)
        )
        ratio = value == "vs_baseline"
        table = data.pivot_table(
            index="cell",
            columns="policy",
            values=value,
            aggfunc=geomean if ratio else "mean",
        )
        table = table[[p for p in self._policy_order(data) if p in table.columns]]
        order = sorted(
            table.index,
            key=lambda cell: (WORKLOADS.index(cell.split()[0]), cell.split()[1:]),
        )
        table = table.loc[order]
        figure, axis = plt.subplots(
            figsize=(1.3 * len(table.columns) + 3, 0.35 * len(table) + 1)
        )
        if ratio:
            sns.heatmap(
                table,
                annot=True,
                fmt=".2f",
                cmap="RdBu",
                center=1.0,
                vmin=0.25,
                vmax=1.75,
                ax=axis,
            )
        else:
            sns.heatmap(table, annot=True, fmt=".0f", cmap="viridis", ax=axis)
        axis.set_title(title)
        axis.set_xlabel("policy")
        axis.set_ylabel("")
        self._add_figure(figure)

    def _mixed_lines(
        self, data: pd.DataFrame, delays: str, x: str, col: str, value: str
    ) -> None:
        labels = {
            "vs_baseline": f"shape geomean relative to {self.mixed_baseline}",
            "affinity": "affinity %",
            "databases": "databases",
        }
        ticks = sorted(set(data[x]))
        grid = sns.relplot(
            data=data,
            kind="line",
            x=x,
            y=value,
            hue="policy",
            hue_order=self._policy_order(data),
            style="policy",
            markers=True,
            dashes=False,
            row="mode",
            col=col,
            height=3.0,
            aspect=1.2,
            facet_kws={"sharey": "row"},
        )
        if x == "databases":
            grid.set(xscale="log")
        grid.set(xticks=ticks)
        for axis in grid.axes.flat:
            axis.minorticks_off()
            axis.set_xticklabels([str(tick) for tick in ticks])
            axis.axhline(1.0, color="black", linewidth=0.8)
        grid.set_axis_labels(labels[x], labels[value])
        grid.figure.suptitle(
            f"Mixed workload, {delays.upper()} delays (one collection for each "
            "database; lower affinity = more sharing between databases)",
            y=1.02,
        )
        self._add_figure(grid.figure)

    def _mixed_shapes(self, data: pd.DataFrame, delays: str) -> None:
        """Adds the throughput of each shape with shared collections, which
        shows the shapes that a policy makes faster and the ones it starves."""
        shared = data[data["affinity"] == data["affinity"].min()]
        long = shared.melt(
            id_vars=["mode", "databases", "policy"],
            value_vars=[f"{name}_tx_per_sec" for name in SHAPES],
            var_name="shape",
            value_name="shape_tx_per_sec",
        )
        long["shape"] = long["shape"].str.removesuffix("_tx_per_sec")
        grid = sns.catplot(
            data=long,
            kind="bar",
            x="shape",
            y="shape_tx_per_sec",
            hue="policy",
            hue_order=self._policy_order(long),
            row="mode",
            col="databases",
            height=2.8,
            aspect=1.1,
            sharey=False,
        )
        grid.set_axis_labels("shape", "tx/s")
        grid.figure.suptitle(
            f"Mixed workload, {delays.upper()} delays: shapes at "
            f"{shared['affinity'].min()}% affinity",
            y=1.02,
        )
        self._add_figure(grid.figure)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--topology-baseline", default="fixed")
    parser.add_argument("--mixed-baseline", default="size")
    parser.add_argument(
        "--image-dir", type=Path, help="also write each figure here as PNG"
    )
    args = parser.parse_args()
    if args.image_dir is not None:
        args.image_dir.mkdir(parents=True, exist_ok=True)
        for old in args.image_dir.glob("*.png"):
            old.unlink()
    baselines = {args.topology_baseline, args.mixed_baseline}
    topology, mixed = load(args.inputs, baselines)
    if topology.empty and mixed.empty:
        parser.error("the inputs have no cells of the selected policies")
    report = Report(
        topology_baseline=args.topology_baseline,
        mixed_baseline=args.mixed_baseline,
        image_dir=args.image_dir,
    )
    report.summary(topology, mixed)
    if not topology.empty:
        report.topology(topology)
    if not mixed.empty:
        report.mixed(mixed)
    args.output.write_text(report.html())


if __name__ == "__main__":
    main()
