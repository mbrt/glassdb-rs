#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///
"""Render the canonical perfbench mixed worker and affinity sweeps."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")
SHAPE_LABELS = {
    "rwSingle": "RW single-key",
    "rwMany": "RW multi-key",
    "roSingle": "RO single-key",
    "roMulti": "RO multi-key",
}
WORKER_POINTS = (1, *range(5, 101, 5))
AFFINITY_POINTS = (0, 25, 50, 75, 100)
AFFINITY_DATABASES = (1, 3, 5, 7)
EXPECTED_RUNS = (1, 2, 3)
FIXED_AFFINITY_WORKERS = 20
WORKER_DATABASE_LIMIT = 5
METRICS = ("throughput", "p50_ms", "p90_ms")


class ReportError(ValueError):
    """The benchmark report cannot support the requested plots."""


@dataclass(frozen=True)
class ReportMetadata:
    backend: str
    model_time_speedup: float


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReportError(f"{field} must be an object")
    return value


def _array(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReportError(f"{field} must be an array")
    return value


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ReportError(f"{field} must be an integer >= {minimum}")
    return value


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReportError(f"{field} must be a number")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ReportError(f"{field} must be finite and nonnegative")
    return result


def read_report(path: Path) -> tuple[ReportMetadata, pd.DataFrame]:
    """Load one version-1 mixed report into one row per run, cell, and shape."""
    try:
        report = _object(json.loads(path.read_text()), str(path))
    except (OSError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error

    if report.get("schemaVersion") != 1 or report.get("scenario") != "mixed":
        raise ReportError(f"{path}: expected a schema-version 1 mixed report")
    backend = report.get("backend")
    if not isinstance(backend, str) or not backend:
        raise ReportError(f"{path}: backend must be a non-empty string")
    metadata = ReportMetadata(
        backend=backend,
        model_time_speedup=_number(
            report.get("modelTimeSpeedup"), f"{path}: modelTimeSpeedup"
        ),
    )

    rows: list[dict[str, Any]] = []
    seen_runs: set[int] = set()
    seen_cells: set[tuple[int, str, int, int, int]] = set()
    for run_index, run_value in enumerate(_array(report.get("runs"), f"{path}: runs")):
        run = _object(run_value, f"{path}: runs[{run_index}]")
        run_id = _integer(run.get("run"), f"{path}: runs[{run_index}].run", minimum=1)
        if run_id in seen_runs:
            raise ReportError(f"{path}: duplicate run {run_id}")
        seen_runs.add(run_id)
        for cell_index, cell_value in enumerate(
            _array(run.get("cells"), f"{path}: run {run_id}.cells")
        ):
            label = f"{path}: run {run_id} cell {cell_index}"
            cell = _object(cell_value, label)
            mode = cell.get("mode")
            if mode not in ("lo", "hi"):
                raise ReportError(f"{label}: mode must be lo or hi")
            affinity = _integer(cell.get("affinityPct"), f"{label}.affinityPct")
            if affinity > 100:
                raise ReportError(f"{label}.affinityPct must not exceed 100")
            database_limit = _integer(
                cell.get("databaseLimit"), f"{label}.databaseLimit", minimum=1
            )
            databases = _integer(
                cell.get("databases"), f"{label}.databases", minimum=1
            )
            workers = _integer(
                cell.get("workersPerShape"), f"{label}.workersPerShape", minimum=1
            )
            if databases != min(database_limit, workers):
                raise ReportError(
                    f"{label}: databases={databases} does not equal "
                    f"min(databaseLimit={database_limit}, workersPerShape={workers})"
                )
            failures = _integer(cell.get("failures"), f"{label}.failures")
            if failures:
                raise ReportError(f"{label}: contains {failures} failures")

            identity = (run_id, mode, affinity, database_limit, workers)
            if identity in seen_cells:
                raise ReportError(f"{label}: duplicate mixed cell {identity}")
            seen_cells.add(identity)

            shape_values = _array(cell.get("shapes"), f"{label}.shapes")
            shapes: dict[str, dict[str, Any]] = {}
            for shape_index, shape_value in enumerate(shape_values):
                shape_label = f"{label}.shapes[{shape_index}]"
                shape = _object(shape_value, shape_label)
                name = shape.get("shape")
                if name not in SHAPES:
                    raise ReportError(f"{shape_label}: unknown shape {name!r}")
                if name in shapes:
                    raise ReportError(f"{label}: duplicate shape {name}")
                if shape.get("converged") is not True:
                    raise ReportError(f"{label}: shape {name} did not converge")
                shapes[name] = shape
            if set(shapes) != set(SHAPES):
                raise ReportError(
                    f"{label}: shapes are {sorted(shapes)}; expected {sorted(SHAPES)}"
                )

            for name in SHAPES:
                shape = shapes[name]
                rows.append(
                    {
                        "run": run_id,
                        "mode": mode,
                        "affinity": affinity,
                        "database_limit": database_limit,
                        "databases": databases,
                        "workers": workers,
                        "shape": name,
                        "throughput": _number(
                            shape.get("txPerSec"), f"{label}.{name}.txPerSec"
                        ),
                        "p50_ms": _number(
                            shape.get("p50Ms"), f"{label}.{name}.p50Ms"
                        ),
                        "p90_ms": _number(
                            shape.get("p90Ms"), f"{label}.{name}.p90Ms"
                        ),
                    }
                )

    if not rows:
        raise ReportError(f"{path}: report has no mixed cells")
    return metadata, pd.DataFrame(rows)


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
    _require_values(
        data, "database_limit", (WORKER_DATABASE_LIMIT,), "worker sweep"
    )
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
        data.groupby([*dimensions, "shape"], as_index=False, sort=True)[
            list(METRICS)
        ]
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


def _plot_shape_series(
    ax: plt.Axes,
    raw: pd.DataFrame,
    medians: pd.DataFrame,
    x: str,
    metric: str,
    colors: dict[str, Any],
) -> None:
    for shape in SHAPES:
        points = raw[raw["shape"] == shape]
        line = medians[medians["shape"] == shape]
        ax.scatter(points[x], points[metric], color=colors[shape], alpha=0.2, s=18)
        ax.plot(
            line[x],
            line[metric],
            color=colors[shape],
            marker="o",
            label=SHAPE_LABELS[shape],
        )


def plot_worker_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["workers"])
    colors = dict(zip(SHAPES, sns.color_palette("colorblind", len(SHAPES)), strict=True))
    fig, ax = plt.subplots(figsize=(10, 6))
    _plot_shape_series(ax, data, medians, "workers", "throughput", colors)
    ax.set_title("Mixed-workload throughput with isolated collections")
    ax.set_xlabel("Concurrent workers per shape")
    ax.set_ylabel("Transactions / sec")
    ax.set_xticks((1, 20, 40, 60, 80, 100))
    ax.legend(title="Transaction shape")
    return _save(fig, out_dir, "worker-throughput.png")


def plot_worker_latency(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["workers"])
    colors = dict(zip(SHAPES, sns.color_palette("colorblind", len(SHAPES)), strict=True))
    fig, axes = plt.subplots(1, 2, figsize=(16, 6), sharex=True)
    for ax, metric, title in zip(
        axes, ("p50_ms", "p90_ms"), ("p50 latency", "p90 latency"), strict=True
    ):
        _plot_shape_series(ax, data, medians, "workers", metric, colors)
        ax.set_title(title)
        ax.set_xlabel("Concurrent workers per shape")
        ax.set_ylabel("Latency (ms)")
        ax.set_xticks((1, 20, 40, 60, 80, 100))
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle("Mixed-workload latency with isolated collections", y=0.99)
    fig.legend(
        handles,
        labels,
        title="Transaction shape",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.91),
        ncol=4,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.72))
    return _save(fig, out_dir, "worker-latency.png")


def _plot_database_series(
    ax: plt.Axes,
    raw: pd.DataFrame,
    medians: pd.DataFrame,
    shape: str,
    metric: str,
    colors: dict[int, Any],
    *,
    labels: bool,
) -> None:
    for databases in AFFINITY_DATABASES:
        points = raw[
            (raw["shape"] == shape) & (raw["database_limit"] == databases)
        ]
        line = medians[
            (medians["shape"] == shape)
            & (medians["database_limit"] == databases)
        ]
        ax.scatter(
            points["affinity"], points[metric], color=colors[databases], alpha=0.2, s=18
        )
        ax.plot(
            line["affinity"],
            line[metric],
            color=colors[databases],
            marker="o",
            label=f"{databases} DB" if labels else None,
        )


def plot_affinity_throughput(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["database_limit", "affinity"])
    colors = dict(
        zip(
            AFFINITY_DATABASES,
            sns.color_palette("colorblind", len(AFFINITY_DATABASES)),
            strict=True,
        )
    )
    fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharex=True)
    for index, (ax, shape) in enumerate(zip(axes.flat, SHAPES, strict=True)):
        _plot_database_series(
            ax, data, medians, shape, "throughput", colors, labels=index == 0
        )
        ax.set_title(SHAPE_LABELS[shape])
        ax.set_xlabel("Home-collection affinity (%)")
        ax.set_ylabel("Transactions / sec")
        ax.set_xticks(AFFINITY_POINTS)
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.suptitle("Throughput by home-collection affinity", y=0.99)
    fig.legend(
        handles,
        labels,
        title="Database clients",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=4,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, "affinity-throughput.png")


def plot_affinity_latency(data: pd.DataFrame, out_dir: Path) -> Path:
    medians = median_rows(data, ["database_limit", "affinity"])
    colors = dict(
        zip(
            AFFINITY_DATABASES,
            sns.color_palette("colorblind", len(AFFINITY_DATABASES)),
            strict=True,
        )
    )
    fig, axes = plt.subplots(2, 4, figsize=(22, 10), sharex=True)
    for row, (metric, percentile) in enumerate((("p50_ms", "p50"), ("p90_ms", "p90"))):
        for column, shape in enumerate(SHAPES):
            ax = axes[row, column]
            _plot_database_series(
                ax,
                data,
                medians,
                shape,
                metric,
                colors,
                labels=row == 0 and column == 0,
            )
            ax.set_title(f"{SHAPE_LABELS[shape]} — {percentile}")
            ax.set_xlabel("Home-collection affinity (%)")
            ax.set_ylabel("Latency (ms)")
            ax.set_xticks(AFFINITY_POINTS)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.suptitle("Latency by home-collection affinity", y=0.99)
    fig.legend(
        handles,
        labels,
        title="Database clients",
        loc="upper center",
        bbox_to_anchor=(0.5, 0.94),
        ncol=4,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.80))
    return _save(fig, out_dir, "affinity-latency.png")


def render(worker_path: Path, affinity_path: Path, out_dir: Path) -> list[Path]:
    """Validate both canonical reports and render their four figures."""
    worker_metadata, workers = read_report(worker_path)
    affinity_metadata, affinities = read_report(affinity_path)
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
    sns.set_theme(style="whitegrid", context="talk")
    return [
        plot_worker_throughput(workers, out_dir),
        plot_worker_latency(workers, out_dir),
        plot_affinity_throughput(affinities, out_dir),
        plot_affinity_latency(affinities, out_dir),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    base = Path(__file__).resolve().parent / "out-sweeps"
    parser.add_argument("--workers", type=Path, default=base / "workers.json")
    parser.add_argument("--affinity", type=Path, default=base / "affinity.json")
    parser.add_argument("--out", type=Path, default=base)
    args = parser.parse_args()
    render(args.workers, args.affinity, args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
