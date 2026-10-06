"""Read `perfbench` result files (schema version 1) into pandas frames.

This module is the only place that knows the on-disk layout of the `perfbench`
JSON envelope. `compare.py` and `plot_results.py` both build on it, so a schema
change needs one edit here instead of one per consumer.

Legacy formats (`mixbench.json`, `rtbench` CSVs) are not read here. Consumers
that still support them adapt them locally.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

SCHEMA_VERSION = 1
SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")


class ReportError(ValueError):
    """A result file does not follow the `perfbench` schema."""


@dataclass(frozen=True)
class ReportMetadata:
    scenario: str
    backend: str
    model_time_speedup: float


def body_replays(cell: dict) -> int:
    # Older perfbench JSON used ``retries`` for body replay counts.
    return cell.get("replays", cell.get("retries", 0))


def body_replays_per_tx(mapping: dict) -> float:
    return mapping.get("replaysPerTx", mapping.get("retriesPerTx", 0))


def read_envelope(path: Path) -> dict[str, Any]:
    """Load one result file and check that it is a supported envelope."""
    return check_envelope(_load(path), str(path))


def check_envelope(
    report: Any, source: str, scenario: str | None = None
) -> dict[str, Any]:
    """Return `report` when it is a supported envelope, optionally of `scenario`."""
    if not isinstance(report, dict) or report.get("schemaVersion") != SCHEMA_VERSION:
        raise ReportError(f"{source}: expected a schema-version 1 perfbench report")
    if not isinstance(report.get("scenario"), str):
        raise ReportError(f"{source}: scenario must be a string")
    if scenario is not None and report["scenario"] != scenario:
        raise ReportError(
            f"{source}: expected a {scenario} report, got {report['scenario']}"
        )
    return report


def metadata(report: dict[str, Any], source: str) -> ReportMetadata:
    backend = report.get("backend")
    if not isinstance(backend, str) or not backend:
        raise ReportError(f"{source}: backend must be a non-empty string")
    return ReportMetadata(
        scenario=report["scenario"],
        backend=backend,
        model_time_speedup=_number(
            report.get("modelTimeSpeedup"), f"{source}: modelTimeSpeedup"
        ),
    )


def runs(report: dict[str, Any], source: str) -> list[tuple[int, dict[str, Any]]]:
    """Return `(run id, run)` pairs, rejecting duplicate run ids."""
    result: list[tuple[int, dict[str, Any]]] = []
    seen: set[int] = set()
    for index, value in enumerate(_array(report.get("runs"), f"{source}: runs")):
        run = _object(value, f"{source}: runs[{index}]")
        run_id = _integer(run.get("run"), f"{source}: runs[{index}].run", minimum=1)
        if run_id in seen:
            raise ReportError(f"{source}: duplicate run {run_id}")
        seen.add(run_id)
        result.append((run_id, run))
    return result


def mixed_cells(report: dict[str, Any], source: str) -> list[dict[str, Any]]:
    """Flatten a mixed report into cells that each carry their `run` id."""
    check_envelope(report, source, "mixed")
    cells: list[dict[str, Any]] = []
    for run_id, run in runs(report, source):
        run_cells = _array(run.get("cells"), f"{source}: run {run_id}.cells")
        for index, value in enumerate(run_cells):
            label = f"{source}: run {run_id} cell {index}"
            cell = _object(value, label)
            failures = _integer(cell.get("failures"), f"{label}.failures")
            if failures:
                raise ReportError(f"{label}: contains {failures} failures")
            cells.append({**cell, "run": run_id})
    return cells


def read_mixed(
    path: Path, *, require_converged: bool
) -> tuple[ReportMetadata, pd.DataFrame]:
    """Load a mixed report into one row per run, cell, and shape.

    With `require_converged`, a shape that hit its time cap before reaching the
    target confidence interval is an error instead of a plotted point.
    """
    source = str(path)
    report = check_envelope(_load(path), source, "mixed")
    meta = metadata(report, source)
    rows: list[dict[str, Any]] = []
    seen_cells: set[tuple[int, str, int, int, int]] = set()
    for cell_index, cell in enumerate(mixed_cells(report, source)):
        run_id = cell["run"]
        label = f"{source}: run {run_id} cell {cell_index}"
        mode = cell.get("mode")
        if mode not in ("lo", "hi"):
            raise ReportError(f"{label}: mode must be lo or hi")
        affinity = _integer(cell.get("affinityPct"), f"{label}.affinityPct")
        if affinity > 100:
            raise ReportError(f"{label}.affinityPct must not exceed 100")
        database_limit = _integer(
            cell.get("databaseLimit"), f"{label}.databaseLimit", minimum=1
        )
        databases = _integer(cell.get("databases"), f"{label}.databases", minimum=1)
        workers = _integer(
            cell.get("workersPerShape"), f"{label}.workersPerShape", minimum=1
        )
        if databases != min(database_limit, workers):
            raise ReportError(
                f"{label}: databases={databases} does not equal "
                f"min(databaseLimit={database_limit}, workersPerShape={workers})"
            )

        identity = (run_id, mode, affinity, database_limit, workers)
        if identity in seen_cells:
            raise ReportError(f"{label}: duplicate mixed cell {identity}")
        seen_cells.add(identity)

        shapes = _cell_shapes(cell, label, require_converged)
        for name in SHAPES:
            shape = shapes[name]
            p50_ms = _number(shape.get("p50Ms"), f"{label}.{name}.p50Ms")
            p90_ms = _number(shape.get("p90Ms"), f"{label}.{name}.p90Ms")
            if p90_ms < p50_ms:
                raise ReportError(
                    f"{label}.{name}: p90Ms={p90_ms} is below p50Ms={p50_ms}"
                )
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
                    "p50_ms": p50_ms,
                    "p90_ms": p90_ms,
                    "converged": shape.get("converged") is True,
                }
            )

    if not rows:
        raise ReportError(f"{source}: report has no mixed cells")
    return meta, pd.DataFrame(rows)


def read_contention(paths: Sequence[Path]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load contention reports into `(latency samples, per-cell stats)`.

    Several files are numbered as consecutive runs, so independently produced
    single-run files can be pooled.
    """
    samples, stats = [], []
    for run_id, source, run in _numbered_runs(paths, "contention"):
        cells = _array(run.get("cells"), f"{source}: run {run_id}.cells")
        for index, value in enumerate(cells):
            label = f"{source}: run {run_id} cell {index}"
            cell = _object(value, label)
            if _integer(cell.get("failures"), f"{label}.failures"):
                raise ReportError(f"{label}: contention cell did not complete cleanly")
            identity = {
                "run": run_id,
                "num-keys": _integer(cell.get("numKeys"), f"{label}.numKeys"),
                "overlap": _integer(cell.get("overlap"), f"{label}.overlap"),
                "overlap-pct": _integer(cell.get("overlapPct"), f"{label}.overlapPct"),
            }
            samples.extend(
                {**identity, "latency-ms": _number(latency, f"{label}.samplesMs")}
                for latency in _array(cell.get("samplesMs", []), f"{label}.samplesMs")
            )
            stats.append(
                {
                    **identity,
                    "count": _integer(cell.get("committed"), f"{label}.committed"),
                    "cell-duration-ms": _number(
                        cell.get("durationMs"), f"{label}.durationMs"
                    ),
                    "tx-per-sec": _number(cell.get("txPerSec"), f"{label}.txPerSec"),
                    "num-replays": body_replays(cell),
                    "direct-candidates": _integer(
                        cell.get("directCandidates"), f"{label}.directCandidates"
                    ),
                    "direct-landed": _integer(
                        cell.get("directLanded"), f"{label}.directLanded"
                    ),
                    "worker-drain-ms": _number(
                        cell.get("workerDrainMs"), f"{label}.workerDrainMs"
                    ),
                }
            )
    return pd.DataFrame(samples), pd.DataFrame(stats)


def read_inline_pressure(paths: Sequence[Path]) -> pd.DataFrame:
    """Load inline-pressure reports into one row per run and phase."""
    fields = {
        "logicalTx": "logical-tx",
        "wallMs": "wall-ms",
        "txPerSec": "tx-per-sec",
        "p50Ms": "p50-ms",
        "p90Ms": "p90-ms",
        "replays": "replays",
        "lockCalls": "lock-calls",
        "directCandidates": "direct-candidates",
        "directLanded": "direct-landed",
        "backendOps": "backend-ops",
        "writeBytes": "write-bytes",
        "splitCandidates": "split-candidates",
        "splitCompleted": "split-completed",
        "splitDeferred": "split-deferred",
        "merges": "merges",
        # Earlier perfbench versions counted the inline pressure splits of
        # ADR-056 in fixed trigger and settle phases.
        "pressureCandidates": "pressure-candidates",
        "pressureCompleted": "pressure-completed",
        "pressureDeferred": "pressure-deferred",
        "pressureDiscarded": "pressure-discarded",
    }
    rows = []
    for run_id, source, run in _numbered_runs(paths, "inline-pressure"):
        phases = _array(run.get("phases"), f"{source}: run {run_id}.phases")
        for index, value in enumerate(phases):
            phase = _object(value, f"{source}: run {run_id} phase {index}")
            row = {"run": run_id, "phase": phase.get("phase")}
            row.update(
                {
                    column: body_replays(phase)
                    if field == "replays"
                    else phase.get(field)
                    for field, column in fields.items()
                }
            )
            rows.append(row)
    return pd.DataFrame(rows)


def _numbered_runs(
    paths: Sequence[Path], scenario: str
) -> list[tuple[int, str, dict[str, Any]]]:
    """Return `(run id, source, run)` for every run in `paths`.

    A single file keeps its own run ids. Several files are numbered as
    consecutive runs, since each one was produced on its own.
    """
    result: list[tuple[int, str, dict[str, Any]]] = []
    for path in paths:
        source = str(path)
        report = check_envelope(_load(path), source, scenario)
        for own_id, run in runs(report, source):
            result.append((len(result) + 1 if len(paths) > 1 else own_id, source, run))
    return result


def _cell_shapes(
    cell: dict[str, Any], label: str, require_converged: bool
) -> dict[str, dict[str, Any]]:
    shapes: dict[str, dict[str, Any]] = {}
    for index, value in enumerate(_array(cell.get("shapes"), f"{label}.shapes")):
        shape_label = f"{label}.shapes[{index}]"
        shape = _object(value, shape_label)
        name = shape.get("shape")
        if name not in SHAPES:
            raise ReportError(f"{shape_label}: unknown shape {name!r}")
        if name in shapes:
            raise ReportError(f"{label}: duplicate shape {name}")
        if require_converged and shape.get("converged") is not True:
            raise ReportError(f"{label}: shape {name} did not converge")
        shapes[name] = shape
    if set(shapes) != set(SHAPES):
        raise ReportError(
            f"{label}: shapes are {sorted(shapes)}; expected {sorted(SHAPES)}"
        )
    return shapes


def _load(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error


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
