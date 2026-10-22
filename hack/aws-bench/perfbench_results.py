"""Read `perfbench` result files (schema version 2) into pandas frames.

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

SCHEMA_VERSION = 2
SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")
# The shapes of the mixed workloads, by their names in the configuration.
SHAPE_NAMES = {
    "rw-single": "rwSingle",
    "rw-many": "rwMany",
    "ro-single": "roSingle",
    "ro-multi": "roMulti",
}
# The mixed workloads, and the mode that the plots and tables name them by.
MIXED_MODES = {"low-conflict": "lo", "high-conflict": "hi"}
# The experiments whose cells are mixed workloads.
MIXED_EXPERIMENTS = ("mixed", "ci", "workers", "affinity")


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
        raise ReportError(f"{source}: expected a schema-version 2 perfbench report")
    if not isinstance(report.get("scenario"), str):
        raise ReportError(f"{source}: scenario must be a string")
    accepted = MIXED_EXPERIMENTS if scenario == "mixed" else (scenario,)
    if scenario is not None and report["scenario"] not in accepted:
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
    """Flatten a mixed report into cells that each carry their `run` id.

    Each cell also carries the `mode` of its workload, and its shapes the
    names of the mixed shapes, as the plots and tables name them.
    """
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
            mode = MIXED_MODES.get(cell.get("workload"))
            if mode is None:
                raise ReportError(
                    f"{label}: workload {cell.get('workload')!r} is not a mixed workload"
                )
            shapes = [
                {**shape, "shape": SHAPE_NAMES.get(shape.get("shape"), shape.get("shape"))}
                for shape in _array(cell.get("shapes"), f"{label}.shapes")
                if isinstance(shape, dict)
            ]
            ops = cell.get("aggregateOps")
            cells.append(
                {
                    **cell,
                    "run": run_id,
                    "mode": mode,
                    "shapes": shapes,
                    "sharedPct": _shared_pct(report, cell),
                    **({"aggregateOps": ops} if ops is not None else {}),
                }
            )
    return cells


def read_mixed(
    path: Path, *, require_converged: bool
) -> tuple[ReportMetadata, pd.DataFrame]:
    """Load a mixed report into one row per run, cell, and shape.

    With `require_converged`, a shape that hit its time cap before reaching the
    target commits per window is an error instead of a plotted point.

    `throughput_p10`, `throughput_p50` and `throughput_p90` are percentiles of
    the shape's throughput windows. Reports written before the windows existed
    have no distribution: their `throughput_p50` is the shape's throughput, and
    their `throughput_p10` and `throughput_p90` are NaN.
    """
    source = str(path)
    report = check_envelope(_load(path), source, "mixed")
    meta = metadata(report, source)
    rows: list[dict[str, Any]] = []
    seen_cells: set[tuple[int, str, int, int, int]] = set()
    for cell_index, cell in enumerate(mixed_cells(report, source)):
        run_id = cell["run"]
        label = f"{source}: run {run_id} cell {cell_index}"
        mode = cell["mode"]
        affinity = _integer(cell.get("affinityPct"), f"{label}.affinityPct")
        if affinity > 100:
            raise ReportError(f"{label}.affinityPct must not exceed 100")
        databases = _integer(cell.get("databases"), f"{label}.databases", minimum=1)
        database_limit = _integer(
            cell.get("databaseLimit"), f"{label}.databaseLimit", minimum=1
        )
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
            throughput = _number(shape.get("txPerSec"), f"{label}.{name}.txPerSec")
            throughput_p10, throughput_p50, throughput_p90 = _window_percentiles(
                shape, f"{label}.{name}"
            )
            if math.isnan(throughput_p50):
                throughput_p50 = throughput
            rows.append(
                {
                    "run": run_id,
                    "mode": mode,
                    "affinity": affinity,
                    "database_limit": database_limit,
                    "databases": databases,
                    "workers": workers,
                    "shape": name,
                    "throughput": throughput,
                    "throughput_p10": throughput_p10,
                    "throughput_p50": throughput_p50,
                    "throughput_p90": throughput_p90,
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
            num_keys, overlap = _overlap(cell, label)
            shapes = _array(cell.get("shapes"), f"{label}.shapes")
            if len(shapes) != 1:
                raise ReportError(f"{label}: expected one contention shape")
            shape = _object(shapes[0], f"{label}.shapes[0]")
            protocol = _object(cell.get("aggregateProtocol"), f"{label}.aggregateProtocol")
            measure = _object(cell.get("measure"), f"{label}.measure")
            committed = _integer(shape.get("committed"), f"{label}.committed")
            identity = {
                "run": run_id,
                "num-keys": num_keys,
                "overlap": overlap,
                "overlap-pct": 100 * overlap // num_keys,
            }
            samples.extend(
                {**identity, "latency-ms": _number(latency, f"{label}.samplesMs")}
                for latency in _array(shape.get("samplesMs", []), f"{label}.samplesMs")
            )
            tx_per_sec = _number(shape.get("txPerSec"), f"{label}.txPerSec")
            stats.append(
                {
                    **identity,
                    "count": committed,
                    "cell-duration-ms": (
                        committed * 1000.0 / tx_per_sec if tx_per_sec else 0.0
                    ),
                    "tx-per-sec": tx_per_sec,
                    "num-replays": _integer(protocol.get("replays"), f"{label}.replays"),
                    "direct-candidates": round(
                        _number(
                            protocol.get("directCandidatesPerTx"),
                            f"{label}.directCandidatesPerTx",
                        )
                        * committed
                    ),
                    "direct-landed": round(
                        _number(
                            protocol.get("directLandedPerTx"), f"{label}.directLandedPerTx"
                        )
                        * committed
                    ),
                    "worker-drain-ms": _number(
                        measure.get("workerDrainMs"), f"{label}.workerDrainMs"
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
        cells = _array(run.get("cells"), f"{source}: run {run_id}.cells")
        for index, value in enumerate(cells):
            phase = _inline_phase(_object(value, f"{source}: run {run_id} cell {index}"))
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


def _inline_phase(cell: dict[str, Any]) -> dict[str, Any]:
    """Return the fields of one phase of an inline-pressure cell."""
    shape = (cell.get("shapes") or [{}])[0]
    ops = cell.get("aggregateOps") or {}
    protocol = cell.get("aggregateProtocol") or {}
    committed = shape.get("committed", 0)
    instances = [
        instance
        for group in cell.get("groups", [])
        for instance in group.get("instances", [])
    ]

    def total(field: str) -> int:
        return sum(instance.get(field, 0) for instance in instances)

    return {
        "phase": (cell.get("labels") or {}).get("phase"),
        "logicalTx": committed,
        "wallMs": (cell.get("measure") or {}).get("wallMs"),
        "txPerSec": shape.get("txPerSec") if committed else None,
        "p50Ms": shape.get("p50Ms") if committed else None,
        "p90Ms": shape.get("p90Ms") if committed else None,
        "replays": protocol.get("replays", 0),
        "lockCalls": protocol.get("lockCalls", 0),
        "directCandidates": total("directCandidates"),
        "directLanded": total("directLanded"),
        "backendOps": ops.get("backendOps"),
        "writeBytes": ops.get("writeBytes"),
        "splitCandidates": total("splitCandidates"),
        "splitCompleted": total("splits"),
        "splitDeferred": total("splitDeferred"),
        "merges": total("merges"),
    }


def _overlap(cell: dict[str, Any], label: str) -> tuple[int, int]:
    """Return the keys and the shared keys of a contention cell."""
    experiment = cell.get("parameters") or {}
    keys = experiment.get("keys")
    shared = experiment.get("shared")
    return (
        _integer(keys, f"{label}.parameters.keys", minimum=1),
        _integer(shared, f"{label}.parameters.shared", minimum=1),
    )


def _shared_pct(report: dict[str, Any], cell: dict[str, Any]) -> int | None:
    """Return the shared percentage of the stripes of a mixed cell."""
    return (cell.get("parameters") or {}).get("sharedPct")


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


def _window_percentiles(
    shape: dict[str, Any], label: str
) -> tuple[float, float, float]:
    """Return the p10, p50 and p90 of a shape's window throughputs, in tx/s."""
    if "windowCounts" not in shape:
        return math.nan, math.nan, math.nan
    counts = [
        _integer(count, f"{label}.windowCounts")
        for count in _array(shape["windowCounts"], f"{label}.windowCounts")
    ]
    window_ms = _number(shape.get("windowMs"), f"{label}.windowMs")
    if not counts or window_ms == 0:
        return math.nan, math.nan, math.nan
    rates = pd.Series(counts, dtype=float) / (window_ms / 1000)
    return (
        float(rates.quantile(0.1)),
        float(rates.quantile(0.5)),
        float(rates.quantile(0.9)),
    )


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
