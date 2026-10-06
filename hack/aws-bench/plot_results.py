#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Plot `perfbench` result files, one set of figures per file.

The scenario recorded in each file selects the figures:

* `mixed`: throughput and latency by shape, one panel per mode. The x-axis is
  the one dimension that varies: affinity or workers per shape;
* `contention`: throughput and latency by number of contended keys.

With several runs in a file, lines show the cross-run median. Pass `--canonical`
to render the four fixed worker and affinity figures of the local S3 model
instead; that mode insists on the complete grids described in the README.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any, Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import perfbench_results
from perfbench_results import SHAPES, ReportError

SHAPE_LABELS = {
    "rwSingle": "RW single-key",
    "rwMany": "RW multi-key",
    "roSingle": "RO single-key",
    "roMulti": "RO multi-key",
}
WORKER_POINTS = (1, *range(10, 201, 10))
WORKER_TICKS = WORKER_POINTS
AFFINITY_POINTS = (0, 25, 50, 75, 100)
AFFINITY_DATABASES = (1, 3, 5, 7)
EXPECTED_RUNS = (1, 2, 3)
FIXED_AFFINITY_WORKERS = 20
WORKER_DATABASE_LIMIT = 5
METRICS = ("throughput", "p50_ms", "p90_ms")
# Dimensions that a generic mixed report may sweep along its x-axis.
MIXED_AXES = {
    "affinity": "Home-collection affinity (%)",
    "workers": "Concurrent workers per shape",
}


def _values(data: pd.DataFrame, column: str) -> tuple[Any, ...]:
    return tuple(sorted(data[column].unique().tolist()))


def _require_values(
    data: pd.DataFrame, column: str, expected: Iterable[Any], label: str
) -> None:
    actual = _values(data, column)
    expected = tuple(expected)
    if actual != expected:
        raise ReportError(f"{label}: {column} values are {actual}; expected {expected}")


def _require_complete_grid(
    data: pd.DataFrame, dimensions: list[str], expected_cells: int, label: str
) -> None:
    counts = data.groupby(["run", *dimensions], sort=False)["shape"].nunique()
    if len(counts) != expected_cells or not (counts == len(SHAPES)).all():
        raise ReportError(f"{label}: report does not contain the complete cell grid")


def validate_worker_sweep(data: pd.DataFrame) -> None:
    """Require the canonical low-contention, isolated worker sweep."""
    _require_values(data, "run", EXPECTED_RUNS, "worker sweep")
    _require_values(data, "mode", ("lo",), "worker sweep")
    _require_values(data, "affinity", (100,), "worker sweep")
    _require_values(data, "database_limit", (WORKER_DATABASE_LIMIT,), "worker sweep")
    _require_values(data, "workers", WORKER_POINTS, "worker sweep")
    _require_complete_grid(
        data,
        ["mode", "affinity", "database_limit", "workers"],
        len(EXPECTED_RUNS) * len(WORKER_POINTS),
        "worker sweep",
    )


def validate_affinity_sweep(data: pd.DataFrame) -> None:
    """Require the canonical low-contention affinity and Database grid."""
    _require_values(data, "run", EXPECTED_RUNS, "affinity sweep")
    _require_values(data, "mode", ("lo",), "affinity sweep")
    _require_values(data, "affinity", AFFINITY_POINTS, "affinity sweep")
    _require_values(data, "database_limit", AFFINITY_DATABASES, "affinity sweep")
    _require_values(data, "workers", (FIXED_AFFINITY_WORKERS,), "affinity sweep")
    _require_complete_grid(
        data,
        ["mode", "affinity", "database_limit", "workers"],
        len(EXPECTED_RUNS) * len(AFFINITY_POINTS) * len(AFFINITY_DATABASES),
        "affinity sweep",
    )


def median_rows(data: pd.DataFrame, dimensions: list[str]) -> pd.DataFrame:
    """Return cross-run median metrics for each plotted series point."""
    return (
        data.groupby([*dimensions, "shape"], as_index=False, sort=True)[list(METRICS)]
        .median()
        .reset_index(drop=True)
    )


def _save(fig: plt.Figure, out_dir: Path, name: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {path}")
    return path


def _shape_colors() -> dict[str, Any]:
    return dict(zip(SHAPES, sns.color_palette("colorblind", len(SHAPES)), strict=True))


def _plot_shape_lines(
    ax: plt.Axes,
    medians: pd.DataFrame,
    x: str,
    metric: str,
    colors: dict[str, Any],
    *,
    labels: bool = True,
) -> None:
    for shape in SHAPES:
        line = medians[medians["shape"] == shape].sort_values(x)
        ax.plot(
            line[x],
            line[metric],
            color=colors[shape],
            label=SHAPE_LABELS[shape] if labels else None,
        )


def _plot_shape_latency_bands(
    ax: plt.Axes,
    medians: pd.DataFrame,
    x: str,
    colors: dict[str, Any],
    *,
    labels: bool = True,
) -> None:
    for shape in SHAPES:
        line = medians[medians["shape"] == shape].sort_values(x)
        x_values = line[x].to_numpy()
        p50_values = line["p50_ms"].to_numpy()
        p90_values = line["p90_ms"].to_numpy()
        ax.fill_between(
            x_values,
            p50_values,
            p90_values,
            color=colors[shape],
            alpha=0.15,
            linewidth=0,
        )
        ax.plot(
            x_values,
            p50_values,
            color=colors[shape],
            label=SHAPE_LABELS[shape] if labels else None,
        )


def plot_worker_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["workers"])
    colors = _shape_colors()
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_shape_lines(ax, medians, "workers", "throughput", colors)
    ax.set_title("Mixed-workload throughput with isolated collections")
    ax.set_xlabel("Concurrent workers per shape")
    ax.set_ylabel("Transactions / sec")
    ax.set_xticks(WORKER_TICKS)
    ax.set_xlim(1, 200)
    ax.tick_params(axis="x", labelrotation=45)
    ax.legend(title="Transaction shape")
    return _save(fig, out_dir, "worker-throughput.png")


def plot_worker_latency(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["workers"])
    colors = _shape_colors()
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_shape_latency_bands(ax, medians, "workers", colors)
    ax.set_title(
        "Mixed-workload latency with isolated collections\np50 line; p50–p90 band"
    )
    ax.set_xlabel("Concurrent workers per shape")
    ax.set_ylabel("Latency (ms)")
    ax.set_xticks(WORKER_TICKS)
    ax.set_xlim(1, 200)
    ax.tick_params(axis="x", labelrotation=45)
    ax.legend(title="Transaction shape")
    return _save(fig, out_dir, "worker-latency.png")


def _database_title(databases: int) -> str:
    suffix = "instance" if databases == 1 else "instances"
    return f"{databases} DB {suffix}"


def plot_affinity_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["database_limit", "affinity"])
    colors = _shape_colors()
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    for index, (ax, databases) in enumerate(
        zip(axes.flat, AFFINITY_DATABASES, strict=True)
    ):
        database_medians = medians[medians["database_limit"] == databases]
        _plot_shape_lines(
            ax,
            database_medians,
            "affinity",
            "throughput",
            colors,
            labels=index == 0,
        )
        ax.set_title(_database_title(databases))
        ax.set_xlabel("Home-collection affinity (%)")
        ax.set_ylabel("Transactions / sec")
        ax.set_xticks(AFFINITY_POINTS)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.suptitle("Throughput by home-collection affinity", y=0.99)
    fig.legend(
        handles,
        labels,
        title="Transaction shape",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=4,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, "affinity-throughput.png")


def plot_affinity_latency(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["database_limit", "affinity"])
    colors = _shape_colors()
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    for index, (ax, databases) in enumerate(
        zip(axes.flat, AFFINITY_DATABASES, strict=True)
    ):
        database_medians = medians[medians["database_limit"] == databases]
        _plot_shape_latency_bands(
            ax,
            database_medians,
            "affinity",
            colors,
            labels=index == 0,
        )
        ax.set_title(_database_title(databases))
        ax.set_xlabel("Home-collection affinity (%)")
        ax.set_ylabel("Latency (ms)")
        ax.set_xticks(AFFINITY_POINTS)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.suptitle("Latency by home-collection affinity — p50 line; p50–p90 band", y=0.99)
    fig.legend(
        handles,
        labels,
        title="Transaction shape",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=4,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, "affinity-latency.png")


def _mixed_axis(data: pd.DataFrame) -> str:
    """Return the one dimension along which a mixed report sweeps."""
    varying = [axis for axis in MIXED_AXES if data[axis].nunique() > 1]
    if len(varying) != 1 or data["database_limit"].nunique() != 1:
        raise ReportError(
            "a mixed report must sweep exactly one of affinity or workers "
            "with a single database limit"
        )
    return varying[0]


def _plot_mode_panels(
    medians: pd.DataFrame,
    axis: str,
    metric: str,
    *,
    title: str,
    bands: bool,
    out_dir: Path,
    name: str,
) -> Path:
    """One panel per mode, sharing the y-axis so modes compare directly."""
    modes = sorted(medians["mode"].unique())
    colors = _shape_colors()
    fig, axes = plt.subplots(
        1, len(modes), figsize=(max(7 * len(modes), 11), 6), sharey=True, squeeze=False
    )
    for index, (ax, mode) in enumerate(zip(axes.flat, modes, strict=True)):
        mode_medians = medians[medians["mode"] == mode]
        if bands:
            _plot_shape_latency_bands(ax, mode_medians, axis, colors, labels=index == 0)
        else:
            _plot_shape_lines(ax, mode_medians, axis, metric, colors, labels=index == 0)
        ax.set_title(f"mode: {mode}")
        ax.set_xlabel(MIXED_AXES[axis])
        ax.set_xticks(sorted(mode_medians[axis].unique()))
        ax.tick_params(axis="x", labelrotation=45)
    axes.flat[0].set_ylabel("Latency (ms)" if bands else "Transactions / sec")
    axes.flat[0].set_ylim(bottom=0)
    fig.suptitle(title)
    fig.legend(
        *axes.flat[0].get_legend_handles_labels(),
        title="Transaction shape",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=len(SHAPES),
    )
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, name)


def plot_mixed_report(path: Path, out_dir: Path) -> list[Path]:
    """Plot throughput and latency of a mixed report against its swept axis."""
    _, data = perfbench_results.read_mixed(path, require_converged=False)
    unconverged = int((~data["converged"]).sum())
    if unconverged:
        print(f"warning: {path}: {unconverged} shape points did not converge")
    axis = _mixed_axis(data)
    medians = median_rows(data, ["mode", axis])
    return [
        _plot_mode_panels(
            medians,
            axis,
            "throughput",
            title="Mixed-workload throughput",
            bands=False,
            out_dir=out_dir,
            name=f"{path.stem}-throughput.png",
        ),
        _plot_mode_panels(
            medians,
            axis,
            "p50_ms",
            title="Mixed-workload latency — p50 line; p50–p90 band",
            bands=True,
            out_dir=out_dir,
            name=f"{path.stem}-latency.png",
        ),
    ]


def plot_contention_report(path: Path, out_dir: Path) -> list[Path]:
    """Plot throughput and latency of a contention report by contended keys.

    Only full-overlap cells are drawn: partial overlaps form a second matrix
    dimension that would turn each figure into a dozen crossing lines.
    """
    samples, stats = perfbench_results.read_contention([path])
    if stats.empty or not (stats["overlap-pct"] == 100).any():
        raise ReportError(f"{path}: report has no 100% overlap cells")
    samples = samples[samples["overlap-pct"] == 100]
    stats = stats[stats["overlap-pct"] == 100]

    latency, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=samples,
        x="num-keys",
        y="latency-ms",
        estimator="median",
        errorbar=("pi", 80),
        marker="o",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Latency under contention\nmedian line; p10–p90 band")
    ax.set_xlabel("Contended keys (100% overlap)")
    ax.set_ylabel("Transaction latency (ms, log scale)")

    throughput, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=stats,
        x="num-keys",
        y="tx-per-sec",
        estimator="median",
        errorbar=None,
        marker="o",
        ax=ax,
    )
    ax.set_title("Throughput under contention")
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Contended keys (100% overlap)")
    ax.set_ylabel("Transactions / sec")
    return [
        _save(latency, out_dir, f"{path.stem}-latency.png"),
        _save(throughput, out_dir, f"{path.stem}-throughput.png"),
    ]


PLOTTERS = {
    "mixed": plot_mixed_report,
    "contention": plot_contention_report,
}


def plot_file(path: Path, out_dir: Path | None) -> list[Path]:
    """Plot one result file into `out_dir`, by default next to the file."""
    scenario = perfbench_results.read_envelope(path)["scenario"]
    plotter = PLOTTERS.get(scenario)
    if plotter is None:
        raise ReportError(f"{path}: no plots for scenario {scenario!r}")
    return plotter(path, out_dir if out_dir is not None else path.parent)


def render(worker_path: Path, affinity_path: Path, out_dir: Path) -> list[Path]:
    """Validate both canonical reports and render their four figures."""
    worker_metadata, workers = perfbench_results.read_mixed(
        worker_path, require_converged=True
    )
    affinity_metadata, affinities = perfbench_results.read_mixed(
        affinity_path, require_converged=True
    )
    if worker_metadata != affinity_metadata:
        raise ReportError(
            "worker and affinity reports must use the same backend and model-time speedup"
        )
    if worker_metadata.backend != "memory" or not math.isclose(
        worker_metadata.model_time_speedup, 5.0
    ):
        raise ReportError(
            "canonical sweeps require --backend=memory and --delay-scale=0.2"
        )
    validate_worker_sweep(workers)
    validate_affinity_sweep(affinities)
    return [
        plot_worker_throughput(workers, out_dir),
        plot_worker_latency(workers, out_dir),
        plot_affinity_throughput(affinities, out_dir),
        plot_affinity_latency(affinities, out_dir),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sweeps = Path(__file__).resolve().parent / "out-sweeps"
    parser.add_argument("files", nargs="*", type=Path, help="perfbench result files")
    parser.add_argument(
        "--out",
        type=Path,
        help="figure directory (default: beside each result file, or out-sweeps)",
    )
    parser.add_argument(
        "--canonical",
        action="store_true",
        help="render the fixed worker and affinity figures instead of FILES",
    )
    parser.add_argument("--workers", type=Path, default=sweeps / "workers.json")
    parser.add_argument("--affinity", type=Path, default=sweeps / "affinity.json")
    args = parser.parse_args()
    if bool(args.files) == args.canonical:
        parser.error("pass result files, or --canonical, but not both")

    sns.set_theme(style="whitegrid", context="talk")
    if args.canonical:
        render(args.workers, args.affinity, args.out or sweeps)
    else:
        for path in args.files:
            plot_file(path, args.out)
    return 0


def run() -> int:
    try:
        return main()
    except ReportError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
