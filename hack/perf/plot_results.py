#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Plot `perfbench` result files, one set of figures per experiment.

The kind of each experiment selects the figures:

* `mixed`, `workers`, and `affinity`: throughput and latency by shape, one panel
  per mode. The x-axis is the one dimension that varies: affinity or workers per
  shape;
* `contention`: throughput and latency by number of contended keys;
* `conflict-sweep`: throughput and latency by probability of conflict, one line
  for each number of keys of a transaction.

A file with several experiments gets figures named `<file>-<experiment>-*.png`.
`--series LABEL=FILE` (at least twice) overlays files of one experiment instead.
Each experiment gets its own figures, named `cmp-<experiment>-*.png`.

Lines show the p50. Bands show the p10 to p90 range of a shape's throughput
windows for throughput, and the p50 to p90 range of its transaction latencies
for latency. With
several runs in a file, lines and band edges show the cross-run median. Pass `--canonical`
to render the four fixed worker and conflict figures of the local S3 model
instead; that mode insists on the complete grids described in the README.
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

import matplotlib

matplotlib.use("Agg")

import matplotlib.ticker

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.lines import Line2D

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
EXPECTED_RUNS = (1, 2, 3)
WORKER_DATABASE_LIMIT = 5
# The cells of `conflict-sweep`: the database instances, the pools of each
# number of keys for a transaction, and the workers.
CONFLICT_DATABASES = (1, 5)
CONFLICT_POOLS = {
    1: (5000, 1000, 500, 200, 100, 50, 20, 10, 5),
    4: (5000, 2000, 1000, 500, 300, 200, 100),
}
CONFLICT_WORKERS = 20
METRICS = (
    "throughput",
    "throughput_p10",
    "throughput_p50",
    "throughput_p90",
    "p50_ms",
    "p90_ms",
)
# The line and band edges of each metric: (line, band low, band high).
THROUGHPUT_BAND = ("throughput_p50", "throughput_p10", "throughput_p90")
LATENCY_BAND = ("p50_ms", "p50_ms", "p90_ms")
THROUGHPUT_BAND_TITLE = "p50 line; p10–p90 band"
LATENCY_BAND_TITLE = "p50 line; p50–p90 band"
# Metrics of a conflict sweep, as `METRICS` for the mixed reports.
CONFLICT_METRICS = (*METRICS, "conflict_probability", "replays_per_tx")
# One line style per overlaid source; shapes keep their colors.
SOURCE_STYLES = ("-", "--", ":", "-.")
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


def validate_conflict_sweep(data: pd.DataFrame) -> None:
    """Require the canonical conflict cells."""
    _require_values(data, "run", EXPECTED_RUNS, "conflict sweep")
    _require_values(data, "databases", CONFLICT_DATABASES, "conflict sweep")
    _require_values(data, "workers", (CONFLICT_WORKERS,), "conflict sweep")
    _require_values(data, "tx_keys", sorted(CONFLICT_POOLS), "conflict sweep")
    for tx_keys, pools in CONFLICT_POOLS.items():
        selected = data[data["tx_keys"] == tx_keys]
        label = f"conflict sweep with {tx_keys} keys"
        _require_values(selected, "pool_keys", sorted(pools), label)
        expected = len(EXPECTED_RUNS) * len(CONFLICT_DATABASES) * len(pools)
        if len(selected) != expected:
            raise ReportError(f"{label}: report does not contain the complete cell grid")


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
    linestyle: str = "-",
) -> None:
    for shape in SHAPES:
        line = medians[medians["shape"] == shape].sort_values(x)
        ax.plot(
            line[x],
            line[metric],
            color=colors[shape],
            linestyle=linestyle,
            label=SHAPE_LABELS[shape] if labels else None,
        )


def _plot_shape_bands(
    ax: plt.Axes,
    medians: pd.DataFrame,
    x: str,
    band: tuple[str, str, str],
    colors: dict[str, Any],
    *,
    labels: bool = True,
) -> None:
    line_column, low_column, high_column = band
    for shape in SHAPES:
        line = medians[medians["shape"] == shape].sort_values(x)
        x_values = line[x].to_numpy()
        low, high = line[low_column].to_numpy(), line[high_column].to_numpy()
        # Reports written before the throughput windows have no band.
        if not pd.isna(high).all():
            ax.fill_between(
                x_values, low, high, color=colors[shape], alpha=0.15, linewidth=0
            )
        ax.plot(
            x_values,
            line[line_column].to_numpy(),
            color=colors[shape],
            label=SHAPE_LABELS[shape] if labels else None,
        )


def plot_worker_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["workers"])
    colors = _shape_colors()
    fig, ax = plt.subplots(figsize=(14, 6))
    _plot_shape_bands(ax, medians, "workers", THROUGHPUT_BAND, colors)
    ax.set_title(
        f"Mixed-workload throughput with isolated collections\n{THROUGHPUT_BAND_TITLE}"
    )
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
    _plot_shape_bands(ax, medians, "workers", LATENCY_BAND, colors)
    ax.set_title(
        f"Mixed-workload latency with isolated collections\n{LATENCY_BAND_TITLE}"
    )
    ax.set_xlabel("Concurrent workers per shape")
    ax.set_ylabel("Latency (ms)")
    ax.set_xticks(WORKER_TICKS)
    ax.set_xlim(1, 200)
    ax.tick_params(axis="x", labelrotation=45)
    ax.legend(title="Transaction shape")
    return _save(fig, out_dir, "worker-latency.png")


def median_conflict_rows(data: pd.DataFrame) -> pd.DataFrame:
    """Return cross-run median metrics for each instance count, transaction
    size, and pool."""
    return (
        data.groupby(["databases", "tx_keys", "pool_keys"], as_index=False, sort=True)[
            list(CONFLICT_METRICS)
        ]
        .median()
        .reset_index(drop=True)
    )


def _conflict_title(metric: str) -> str:
    """Return the two-line title: the metric, then the bands and the log scale."""
    band = THROUGHPUT_BAND_TITLE if metric == "throughput" else LATENCY_BAND_TITLE
    name = "Throughput" if metric == "throughput" else "Latency"
    return f"{name} by p(conflict)\n{band} (log scale)"


def _plot_conflict(data: pd.DataFrame, out_dir: Path, metric: str, name: str) -> Path:
    """One panel for each instance count, stacked and sharing both axes."""
    band = THROUGHPUT_BAND if metric == "throughput" else LATENCY_BAND
    line_column, low_column, high_column = band
    medians = median_conflict_rows(data)
    sizes = sorted(medians["tx_keys"].unique())
    colors = dict(zip(sizes, sns.color_palette("colorblind", len(sizes)), strict=True))
    instances = sorted(medians["databases"].unique())
    fig, axes = plt.subplots(
        len(instances),
        1,
        figsize=(12, 4.5 * len(instances)),
        sharex=True,
        sharey=True,
        squeeze=False,
    )
    for ax, databases in zip(axes.flat, instances, strict=True):
        for size in sizes:
            line = medians[
                (medians["databases"] == databases) & (medians["tx_keys"] == size)
            ].sort_values("conflict_probability")
            x = 100 * line["conflict_probability"].to_numpy()
            low, high = line[low_column].to_numpy(), line[high_column].to_numpy()
            if not pd.isna(high).all():
                ax.fill_between(
                    x, low, high, color=colors[size], alpha=0.15, linewidth=0
                )
            ax.plot(
                x,
                line[line_column].to_numpy(),
                color=colors[size],
                marker="o",
                label=f"{size} key" + ("" if size == 1 else "s"),
            )
        ax.set_xlim(0, 100)
        ax.set_xticks(range(0, 101, 10))
        ax.xaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda value, _: f"{value:g}%")
        )
        ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        ax.set_title(f"{databases} DB instance" + ("" if databases == 1 else "s"))
        ax.set_ylabel(
            "Transactions / sec" if metric == "throughput" else "Latency (ms)"
        )
    axes.flat[-1].set_xlabel("Probability of conflict")
    axes.flat[0].set_yscale("log")
    axes.flat[0].legend(title="Keys per transaction")
    fig.suptitle(_conflict_title(metric))
    fig.tight_layout()
    return _save(fig, out_dir, name)


def plot_conflict_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    return _plot_conflict(data, out_dir, "throughput", "conflict-throughput.png")


def plot_conflict_latency(data: pd.DataFrame, out_dir: Path) -> Path:
    return _plot_conflict(data, out_dir, "latency", "conflict-latency.png")


def plot_conflict_report(
    series: Mapping[str, Path], out_dir: Path, stem: str, name: str | None = None
) -> list[Path]:
    """Plot throughput and latency of a conflict sweep against the probability
    of conflict. It does not overlay several files."""
    if len(series) != 1:
        raise ReportError("a conflict sweep cannot be overlaid")
    path = next(iter(series.values()))
    _, data = perfbench_results.read_conflict_sweep(
        path, require_converged=False, name=name
    )
    unconverged = int((~data["converged"]).sum())
    if unconverged:
        print(f"warning: {path}: {unconverged} cells did not converge")
    return [
        _plot_conflict(data, out_dir, "throughput", f"{stem}-throughput.png"),
        _plot_conflict(data, out_dir, "latency", f"{stem}-latency.png"),
    ]


def _mixed_axis(data: pd.DataFrame) -> str:
    """Return the one dimension along which a mixed report sweeps."""
    varying = [axis for axis in MIXED_AXES if data[axis].nunique() > 1]
    if len(varying) != 1 or data["database_limit"].nunique() != 1:
        raise ReportError(
            "a mixed report must sweep exactly one of affinity or workers "
            "with a single database limit"
        )
    return varying[0]


def _source_legend(fig: plt.Figure, sources: list[str]) -> None:
    """Legend for shape colors, plus line styles when sources are overlaid."""
    colors = _shape_colors()
    handles = [
        Line2D([], [], color=colors[shape], label=SHAPE_LABELS[shape])
        for shape in SHAPES
    ]
    if len(sources) > 1:
        handles += [
            Line2D(
                [],
                [],
                color="black",
                linestyle=SOURCE_STYLES[index % len(SOURCE_STYLES)],
                label=source,
            )
            for index, source in enumerate(sources)
        ]
    fig.legend(
        handles=handles,
        title="Transaction shape" + (" / source" if len(sources) > 1 else ""),
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=len(handles),
    )


def _plot_mode_panels(
    medians: pd.DataFrame,
    axis: str,
    band: tuple[str, str, str],
    *,
    title: str,
    out_dir: Path,
    name: str,
) -> Path:
    """One panel per mode, sharing the y-axis so modes compare directly.

    Overlaid sources are told apart by line style. Bands are drawn only for a
    single source, where they stay readable.
    """
    modes = sorted(medians["mode"].unique())
    sources = list(dict.fromkeys(medians["source"]))
    bands = len(sources) == 1
    colors = _shape_colors()
    fig, axes = plt.subplots(
        1, len(modes), figsize=(max(7 * len(modes), 11), 6), sharey=True, squeeze=False
    )
    for ax, mode in zip(axes.flat, modes, strict=True):
        for index, source in enumerate(sources):
            selected = medians[
                (medians["mode"] == mode) & (medians["source"] == source)
            ]
            if bands:
                _plot_shape_bands(ax, selected, axis, band, colors, labels=False)
            else:
                _plot_shape_lines(
                    ax,
                    selected,
                    axis,
                    band[0],
                    colors,
                    labels=False,
                    linestyle=SOURCE_STYLES[index % len(SOURCE_STYLES)],
                )
        ax.set_title(f"mode: {mode}")
        ax.set_xlabel(MIXED_AXES[axis])
        ax.set_xticks(sorted(medians[medians["mode"] == mode][axis].unique()))
        ax.tick_params(axis="x", labelrotation=45)
    axes.flat[0].set_ylabel(
        "Latency (ms)" if band == LATENCY_BAND else "Transactions / sec"
    )
    axes.flat[0].set_ylim(bottom=0)
    fig.suptitle(title)
    _source_legend(fig, sources)
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, name)


def plot_mixed_report(
    series: Mapping[str, Path], out_dir: Path, stem: str, name: str | None = None
) -> list[Path]:
    """Plot throughput and latency of mixed reports against their swept axis."""
    frames, axes = [], set()
    for label, path in series.items():
        _, data = perfbench_results.read_mixed(
            path, require_converged=False, name=name
        )
        unconverged = int((~data["converged"]).sum())
        if unconverged:
            print(f"warning: {path}: {unconverged} shape points did not converge")
        axes.add(_mixed_axis(data))
        frames.append(data.assign(source=label))
    if len(axes) != 1:
        raise ReportError("mixed reports must sweep the same dimension to overlay")
    axis = axes.pop()
    medians = median_rows(
        pd.concat(frames, ignore_index=True), ["source", "mode", axis]
    )
    overlay = len(series) > 1
    return [
        _plot_mode_panels(
            medians,
            axis,
            THROUGHPUT_BAND,
            title="Mixed-workload throughput"
            + ("" if overlay else f" — {THROUGHPUT_BAND_TITLE}"),
            out_dir=out_dir,
            name=f"{stem}-throughput.png",
        ),
        _plot_mode_panels(
            medians,
            axis,
            LATENCY_BAND,
            title="Mixed-workload latency — "
            + ("p50" if overlay else LATENCY_BAND_TITLE),
            out_dir=out_dir,
            name=f"{stem}-latency.png",
        ),
    ]


def plot_contention_report(
    series: Mapping[str, Path], out_dir: Path, stem: str, name: str | None = None
) -> list[Path]:
    """Plot throughput and latency of contention reports by contended keys.

    Only full-overlap cells are drawn: partial overlaps form a second matrix
    dimension that would turn each figure into a dozen crossing lines.
    """
    frames = []
    for label, path in series.items():
        stats = perfbench_results.read_contention([path])
        if stats.empty or not (stats["overlap-pct"] == 100).any():
            raise ReportError(f"{path}: report has no 100% overlap cells")
        frames.append(stats[stats["overlap-pct"] == 100].assign(source=label))
    stats = pd.concat(frames, ignore_index=True)
    style = "source" if len(series) > 1 else None

    percentiles = stats.melt(
        id_vars=["source", "num-keys"],
        value_vars=["p50-ms", "p90-ms"],
        var_name="percentile",
        value_name="latency-ms",
    )
    latency, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=percentiles,
        x="num-keys",
        y="latency-ms",
        hue="percentile",
        style=style,
        estimator="median",
        errorbar=None,
        marker="o",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Latency under contention\nmedian over runs of each percentile")
    ax.set_xlabel("Contended keys (100% overlap)")
    ax.set_ylabel("Transaction latency (ms, log scale)")

    throughput, ax = plt.subplots(figsize=(8, 5))
    sns.lineplot(
        data=stats,
        x="num-keys",
        y="tx-per-sec",
        style=style,
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
        _save(latency, out_dir, f"{stem}-latency.png"),
        _save(throughput, out_dir, f"{stem}-throughput.png"),
    ]


PLOTTERS = {
    "mixed": plot_mixed_report,
    "conflict-sweep": plot_conflict_report,
    "contention": plot_contention_report,
}


def plot_file(path: Path, out_dir: Path | None) -> list[Path]:
    """Plot each experiment of one result file into `out_dir`, by default next
    to the file."""
    out_dir = out_dir or path.parent
    names = perfbench_results.experiment_names(path)
    if len(names) == 1:
        return plot_series({path.stem: path}, out_dir, path.stem)
    outputs = []
    for name in names:
        if _kind(name) in PLOTTERS:
            stem = f"{path.stem}-{name}"
            outputs += plot_series({path.stem: path}, out_dir, stem, name)
    if not outputs:
        raise ReportError(f"{path}: no plots for experiments {names}")
    return outputs


def plot_overlay(
    series: Mapping[str, Path], out_dir: Path, name: str | None = None
) -> list[Path]:
    """Overlay the experiment `name` of result files, by default their one
    experiment."""
    scenario = _scenario(next(iter(series.values())), name)
    return plot_series(series, out_dir, f"cmp-{scenario}", name)


def plot_series(
    series: Mapping[str, Path], out_dir: Path, stem: str, name: str | None = None
) -> list[Path]:
    scenarios = {_scenario(path, name) for path in series.values()}
    if len(scenarios) != 1:
        raise ReportError(f"cannot overlay different experiments: {sorted(scenarios)}")
    scenario = scenarios.pop()
    plotter = PLOTTERS.get(_kind(scenario))
    if plotter is None:
        raise ReportError(f"no plots for experiment {scenario!r}")
    return plotter(series, out_dir, stem, scenario)


def _kind(name: str) -> str:
    """Return the kind of plots of the experiment `name`."""
    return "mixed" if name in perfbench_results.MIXED_EXPERIMENTS else name


def _scenario(path: Path, name: str | None) -> str:
    return perfbench_results.read_envelope(path, name)["scenario"]


def render(worker_path: Path, conflict_path: Path, out_dir: Path) -> list[Path]:
    """Validate both canonical reports and render their four figures."""
    worker_metadata, workers = perfbench_results.read_mixed(
        worker_path, require_converged=True, name="workers"
    )
    conflict_metadata, conflicts = perfbench_results.read_conflict_sweep(
        conflict_path, require_converged=True, name="conflict-sweep"
    )
    if _model(worker_metadata) != _model(conflict_metadata):
        raise ReportError(
            "worker and conflict reports must use the same backend and time model"
        )
    if (
        worker_metadata.backend != "memory"
        or worker_metadata.time_mode != "real"
        or not math.isclose(worker_metadata.model_time_speedup, 1.0)
    ):
        raise ReportError(
            "canonical sweeps require --backend=memory in real time, at the speed of wall time"
        )
    validate_worker_sweep(workers)
    validate_conflict_sweep(conflicts)
    return [
        plot_worker_throughput(workers, out_dir),
        plot_worker_latency(workers, out_dir),
        plot_conflict_throughput(conflicts, out_dir),
        plot_conflict_latency(conflicts, out_dir),
    ]


def _model(meta: perfbench_results.ReportMetadata) -> tuple[str, str, float]:
    """Return the backend, the time mode, and the model-time speedup of a report."""
    return meta.backend, meta.time_mode, meta.model_time_speedup


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
        "--series",
        action="append",
        default=[],
        metavar="LABEL=FILE",
        help="overlay result files of one experiment (repeat; needs --out)",
    )
    parser.add_argument(
        "--canonical",
        action="store_true",
        help="render the fixed worker and conflict figures instead of FILES",
    )
    parser.add_argument("--workers", type=Path, default=sweeps / "workers.json")
    parser.add_argument(
        "--conflicts", type=Path, default=sweeps / "conflict-sweep.json"
    )
    args = parser.parse_args()
    modes = [bool(args.files), bool(args.series), args.canonical]
    if sum(modes) != 1:
        parser.error("pass result files, or --series, or --canonical")
    if args.series and (len(args.series) < 2 or args.out is None):
        parser.error("--series needs at least two entries and --out")

    sns.set_theme(style="whitegrid", context="talk")
    if args.canonical:
        render(args.workers, args.conflicts, args.out or sweeps)
    elif args.series:
        plot_overlay(_parse_series(parser, args.series), args.out)
    else:
        for path in args.files:
            plot_file(path, args.out)
    return 0


def _parse_series(
    parser: argparse.ArgumentParser, entries: list[str]
) -> dict[str, Path]:
    series: dict[str, Path] = {}
    for entry in entries:
        label, separator, path = entry.partition("=")
        if not separator or not label or label in series:
            parser.error(f"--series expects unique LABEL=FILE entries, got {entry!r}")
        series[label] = Path(path)
    return series


def run() -> int:
    try:
        return main()
    except ReportError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
