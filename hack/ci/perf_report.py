#!/usr/bin/env python3
"""Render a compact Markdown comparison from PR performance JSON artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

SCORE_RUNS = 11
MIX_RUNS = 3
DEADLOCK_RUNS = 3
WORKLOADS = (
    "singleRMW",
    "multiRMW10",
    "batchRead10",
    "batchWrite100",
    "readRepeat",
)
SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")
SECONDARY = (
    ("Allocation bytes/tx", "allocBytesPerTx", 0),
    ("Allocations/tx", "allocsPerTx", 1),
    ("Wall ns/tx", "nsPerTx", 0),
    ("CPU ns/tx", "cpuNsPerTx", 0),
)


class ReportError(ValueError):
    """The result artifact is incomplete or has an incompatible schema."""


@dataclass(frozen=True)
class ScoreRun:
    score: float
    workloads: dict[str, float]
    secondary: dict[str, float]


@dataclass(frozen=True)
class MixShape:
    tx_per_sec: float
    p50_ms: float
    p90_ms: float
    converged: bool
    committed: int
    relative_ci: float


@dataclass(frozen=True)
class MixRun:
    shapes: dict[str, MixShape]
    total_ops_per_tx: float
    retries_per_tx: float


@dataclass(frozen=True)
class DeadlockRun:
    completed: int
    tx_per_sec: float | None
    p50_ms: float
    p90_ms: float
    retries_per_tx: float | None
    direct_candidates_per_tx: float | None
    direct_land_rate: float | None
    worker_drain_ms: float | None


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReportError(f"{field} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ReportError(f"{field} must be finite")
    return result


def _nonnegative(value: Any, field: str) -> float:
    result = _number(value, field)
    if result < 0:
        raise ReportError(f"{field} must be nonnegative")
    return result


def _count(value: Any, field: str) -> int:
    result = _nonnegative(value, field)
    if not result.is_integer():
        raise ReportError(f"{field} must be an integer")
    return int(result)


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReportError(f"{field} must be an object")
    return value


def _array(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReportError(f"{field} must be an array")
    return value


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error


def _result_files(path: Path, count: int) -> list[Path]:
    files = sorted(path.glob("*.json"))
    if len(files) != count:
        raise ReportError(f"{path} contains {len(files)} JSON files; expected {count}")
    return files


def load_score_runs(path: Path) -> list[ScoreRun]:
    runs = []
    for result_path in _result_files(path, SCORE_RUNS):
        raw = _object(_read_json(result_path), str(result_path))
        workload_rows = _array(raw.get("workloads"), f"{result_path}: workloads")
        workloads: dict[str, float] = {}
        for index, row_value in enumerate(workload_rows):
            row = _object(row_value, f"{result_path}: workloads[{index}]")
            name = row.get("name")
            if not isinstance(name, str):
                raise ReportError(f"{result_path}: workload name must be a string")
            if name in workloads:
                raise ReportError(f"{result_path}: duplicate workload {name}")
            workloads[name] = _nonnegative(
                row.get("costPerTx"), f"{result_path}: {name}.costPerTx"
            )
        if set(workloads) != set(WORKLOADS):
            raise ReportError(
                f"{result_path}: workloads are {sorted(workloads)}; "
                f"expected {sorted(WORKLOADS)}"
            )
        secondary_raw = _object(raw.get("secondary"), f"{result_path}: secondary")
        secondary = {
            field: _nonnegative(secondary_raw.get(field), f"{result_path}: {field}")
            for _, field, _ in SECONDARY
        }
        runs.append(
            ScoreRun(
                score=_nonnegative(raw.get("score"), f"{result_path}: score"),
                workloads=workloads,
                secondary=secondary,
            )
        )
    return runs


def load_mix_runs(path: Path) -> list[MixRun]:
    runs = []
    for result_path in _result_files(path, MIX_RUNS):
        cells = _array(_read_json(result_path), str(result_path))
        if len(cells) != 1:
            raise ReportError(f"{result_path}: expected one mixbench cell")
        cell = _object(cells[0], f"{result_path}: cell")
        if cell.get("mode") != "hi" or cell.get("topology") != "shared":
            raise ReportError(f"{result_path}: expected the hi/shared cell")
        failures = _count(cell.get("failures"), f"{result_path}: failures")
        if failures != 0:
            raise ReportError(f"{result_path}: mixbench recorded {failures:g} failures")

        shapes: dict[str, MixShape] = {}
        for index, row_value in enumerate(
            _array(cell.get("shapes"), f"{result_path}: shapes")
        ):
            row = _object(row_value, f"{result_path}: shapes[{index}]")
            name = row.get("shape")
            if not isinstance(name, str):
                raise ReportError(f"{result_path}: shape name must be a string")
            if name in shapes:
                raise ReportError(f"{result_path}: duplicate shape {name}")
            converged = row.get("converged")
            if not isinstance(converged, bool):
                raise ReportError(f"{result_path}: {name}.converged must be boolean")
            shapes[name] = MixShape(
                tx_per_sec=_nonnegative(
                    row.get("txPerSec"), f"{result_path}: {name}.txPerSec"
                ),
                p50_ms=_nonnegative(row.get("p50Ms"), f"{result_path}: {name}.p50Ms"),
                p90_ms=_nonnegative(row.get("p90Ms"), f"{result_path}: {name}.p90Ms"),
                converged=converged,
                committed=_count(
                    row.get("committed"), f"{result_path}: {name}.committed"
                ),
                relative_ci=_nonnegative(
                    row.get("relCi"), f"{result_path}: {name}.relCi"
                ),
            )
        if set(shapes) != set(SHAPES):
            raise ReportError(
                f"{result_path}: shapes are {sorted(shapes)}; expected {sorted(SHAPES)}"
            )
        aggregate = _object(cell.get("aggregateOps"), f"{result_path}: aggregateOps")
        runs.append(
            MixRun(
                shapes=shapes,
                total_ops_per_tx=_nonnegative(
                    aggregate.get("totalOpsPerTx"),
                    f"{result_path}: aggregateOps.totalOpsPerTx",
                ),
                retries_per_tx=_nonnegative(
                    aggregate.get("retriesPerTx"),
                    f"{result_path}: aggregateOps.retriesPerTx",
                ),
            )
        )
    return runs


def _percentile_r8(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise ReportError("cannot take a percentile of no samples")
    n = 1.0 / 3.0 + percentile * (len(ordered) + 1.0 / 3.0)
    k = math.floor(n)
    fraction = n - k
    if k <= 0:
        return ordered[0]
    if k >= len(ordered):
        return ordered[-1]
    return ordered[k - 1] + fraction * (ordered[k] - ordered[k - 1])


def _csv_rows(path: Path) -> list[dict[str, str]]:
    try:
        with path.open(newline="") as source:
            return list(csv.DictReader(source))
    except OSError as error:
        raise ReportError(f"cannot read {path}: {error}") from error


def _csv_number(row: dict[str, str], field: str, path: Path) -> float:
    raw = row.get(field)
    try:
        value = float(raw) if raw is not None else float("nan")
    except ValueError as error:
        raise ReportError(f"{path}: {field} must be a number") from error
    return _nonnegative(value, f"{path}: {field}")


def load_deadlock_runs(path: Path, *, require_stats: bool) -> list[DeadlockRun]:
    raw_files = sorted(
        result
        for result in path.glob("*.csv")
        if not result.name.endswith("-stats.csv")
    )
    expected_names = [f"{run:02d}.csv" for run in range(1, DEADLOCK_RUNS + 1)]
    if [result.name for result in raw_files] != expected_names:
        raise ReportError(
            f"{path} must contain paired deadlock runs {expected_names}; "
            f"found {[result.name for result in raw_files]}"
        )
    stats_files = [
        result.with_name(f"{result.stem}-stats.csv") for result in raw_files
    ]
    stats_present = [result.exists() for result in stats_files]
    if require_stats and not all(stats_present):
        raise ReportError(f"{path}: focused deadlock stats are required for every run")
    if any(stats_present) and not all(stats_present):
        raise ReportError(f"{path}: focused deadlock stats are incomplete")

    runs = []
    for result_path in raw_files:
        rows = [
            row
            for row in _csv_rows(result_path)
            if _csv_number(row, "num-keys", result_path) == 1
            and _csv_number(row, "overlap-pct", result_path) == 100
        ]
        latencies = [
            _csv_number(row, "latency-ms", result_path)
            for row in rows
        ]
        if not latencies:
            raise ReportError(f"{result_path}: no one-key full-overlap samples")

        stats_path = result_path.with_name(f"{result_path.stem}-stats.csv")
        stats_rows = []
        if stats_path.exists():
            stats_rows = [
                row
                for row in _csv_rows(stats_path)
                if _csv_number(row, "num-keys", stats_path) == 1
                and _csv_number(row, "overlap-pct", stats_path) == 100
            ]
            if len(stats_rows) != 1:
                raise ReportError(
                    f"{stats_path}: expected one one-key full-overlap row"
                )

        tx_per_sec = retries_per_tx = direct_candidates_per_tx = None
        direct_land_rate = worker_drain_ms = None
        if stats_rows:
            row = stats_rows[0]
            completed = _count(
                _csv_number(row, "count", stats_path), f"{stats_path}: count"
            )
            if completed != len(latencies):
                raise ReportError(
                    f"{stats_path}: count={completed} but {result_path} has "
                    f"{len(latencies)} samples"
                )
            duration_ms = _csv_number(row, "cell-duration-ms", stats_path)
            if duration_ms == 0:
                raise ReportError(f"{stats_path}: cell-duration-ms must be positive")
            candidates_field = (
                "direct-candidates"
                if "direct-candidates" in row
                else "direct-attempts"
            )
            landed_field = (
                "direct-landed" if "direct-landed" in row else "direct-commits"
            )
            candidates = _csv_number(row, candidates_field, stats_path)
            landed = _csv_number(row, landed_field, stats_path)
            if landed > candidates:
                raise ReportError(
                    f"{stats_path}: direct landed count exceeds candidates"
                )
            tx_per_sec = completed * 1000.0 / duration_ms
            retries_per_tx = (
                _csv_number(row, "num-retries", stats_path) / completed
            )
            direct_candidates_per_tx = candidates / completed
            direct_land_rate = (
                landed / candidates if candidates > 0 else None
            )
            worker_drain_ms = _csv_number(row, "worker-drain-ms", stats_path)

        runs.append(
            DeadlockRun(
                completed=len(latencies),
                tx_per_sec=tx_per_sec,
                p50_ms=_percentile_r8(latencies, 0.5),
                p90_ms=_percentile_r8(latencies, 0.9),
                retries_per_tx=retries_per_tx,
                direct_candidates_per_tx=direct_candidates_per_tx,
                direct_land_rate=direct_land_rate,
                worker_drain_ms=worker_drain_ms,
            )
        )
    return runs


def _values(items: Iterable[Any], field: str) -> list[float]:
    return [float(getattr(item, field)) for item in items]


def _summary(values: Iterable[float], digits: int = 2) -> str:
    samples = list(values)
    median = statistics.median(samples)
    low, high = min(samples), max(samples)
    if math.isclose(low, high):
        return f"{median:,.{digits}f}"
    return f"{median:,.{digits}f} ({low:,.{digits}f}–{high:,.{digits}f})"


def _change(base: Iterable[float], candidate: Iterable[float]) -> str:
    base_median = statistics.median(base)
    candidate_median = statistics.median(candidate)
    if base_median == 0:
        return "n/a"
    return f"{(candidate_median / base_median - 1.0) * 100:+.2f}%"


def _available(items: Iterable[float | int | None]) -> list[float]:
    return [float(item) for item in items if item is not None]


def _optional_summary(
    items: Iterable[float | int | None], digits: int = 2
) -> str:
    values = _available(items)
    return _summary(values, digits) if values else "n/a"


def _optional_change(
    base: Iterable[float | int | None],
    candidate: Iterable[float | int | None],
) -> str:
    base_values = _available(base)
    candidate_values = _available(candidate)
    if not base_values or not candidate_values:
        return "n/a"
    return _change(base_values, candidate_values)


def _escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_report(input_dir: Path, base_label: str, candidate_label: str) -> str:
    base_scores = load_score_runs(input_dir / "score" / "main")
    candidate_scores = load_score_runs(input_dir / "score" / "pr")
    base_mix = load_mix_runs(input_dir / "mix" / "main")
    candidate_mix = load_mix_runs(input_dir / "mix" / "pr")
    base_deadlock = load_deadlock_runs(
        input_dir / "deadlock" / "main", require_stats=False
    )
    candidate_deadlock = load_deadlock_runs(
        input_dir / "deadlock" / "pr", require_stats=True
    )

    base_score_values = _values(base_scores, "score")
    candidate_score_values = _values(candidate_scores, "score")
    lines = [
        "# Performance comparison",
        "",
        f"- Base: `{_escape(base_label)}`",
        f"- Candidate: `{_escape(candidate_label)}`",
        "",
        "## Backend-operation score",
        "",
        "Lower is better. Values are medians with the observed min–max range "
        "from 11 interleaved runs.",
        "",
        "| Metric | Main | PR | Change |",
        "| --- | ---: | ---: | ---: |",
        f"| Primary weighted cost/tx | {_summary(base_score_values)} | "
        f"{_summary(candidate_score_values)} | "
        f"{_change(base_score_values, candidate_score_values)} |",
    ]
    for workload in WORKLOADS:
        base = [run.workloads[workload] for run in base_scores]
        candidate = [run.workloads[workload] for run in candidate_scores]
        lines.append(
            f"| `{workload}` cost/tx | {_summary(base)} | {_summary(candidate)} | "
            f"{_change(base, candidate)} |"
        )

    lines.extend(
        [
            "",
            "> The primary is count-based, but background protocol work currently "
            "gives it an observed noise floor of roughly 1%; small changes should be "
            "treated as unchanged.",
            "",
            "<details>",
            "<summary>Noisy in-memory secondary axes</summary>",
            "",
            "| Metric | Main | PR | Change |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for label, field, digits in SECONDARY:
        base = [run.secondary[field] for run in base_scores]
        candidate = [run.secondary[field] for run in candidate_scores]
        lines.append(
            f"| {label} | {_summary(base, digits)} | {_summary(candidate, digits)} | "
            f"{_change(base, candidate)} |"
        )
    lines.extend(["", "</details>", "", "## Focused contention mix", ""])

    unconverged = []
    for side, runs in (("main", base_mix), ("PR", candidate_mix)):
        for repetition, run in enumerate(runs, start=1):
            missing = [
                name for name, shape in run.shapes.items() if not shape.converged
            ]
            if missing:
                unconverged.append(
                    f"{side} run {repetition}: {', '.join(sorted(missing))}"
                )
    if unconverged:
        lines.append(
            "⚠️ Some shapes hit the time cap before reaching the requested "
            "throughput confidence target: " + "; ".join(unconverged) + "."
        )
    else:
        lines.append("All shapes converged in all three runs on both revisions.")
    lines.extend(
        [
            "",
            "One shared Database, eight workers per shape, and eight hot keys. "
            "Values are medians with min–max ranges; throughput is higher-is-better, "
            "while latency is lower-is-better.",
            "",
            "| Shape | Main tx/s | PR tx/s | Change | Main p50 ms | PR p50 ms | "
            "p50 change | Main p90 ms | PR p90 ms | p90 change |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for shape_name in SHAPES:
        base_shapes = [run.shapes[shape_name] for run in base_mix]
        candidate_shapes = [run.shapes[shape_name] for run in candidate_mix]
        base_tps = _values(base_shapes, "tx_per_sec")
        candidate_tps = _values(candidate_shapes, "tx_per_sec")
        base_p50 = _values(base_shapes, "p50_ms")
        candidate_p50 = _values(candidate_shapes, "p50_ms")
        base_p90 = _values(base_shapes, "p90_ms")
        candidate_p90 = _values(candidate_shapes, "p90_ms")
        lines.append(
            f"| `{shape_name}` | {_summary(base_tps)} | {_summary(candidate_tps)} | "
            f"{_change(base_tps, candidate_tps)} | {_summary(base_p50, 1)} | "
            f"{_summary(candidate_p50, 1)} | {_change(base_p50, candidate_p50)} | "
            f"{_summary(base_p90, 1)} | {_summary(candidate_p90, 1)} | "
            f"{_change(base_p90, candidate_p90)} |"
        )

    base_ops = _values(base_mix, "total_ops_per_tx")
    candidate_ops = _values(candidate_mix, "total_ops_per_tx")
    base_retries = _values(base_mix, "retries_per_tx")
    candidate_retries = _values(candidate_mix, "retries_per_tx")
    lines.extend(
        [
            "",
            "| Aggregate metric | Main | PR | Change |",
            "| --- | ---: | ---: | ---: |",
            f"| Backend ops/tx | {_summary(base_ops, 3)} | "
            f"{_summary(candidate_ops, 3)} | {_change(base_ops, candidate_ops)} |",
            f"| Retries/tx | {_summary(base_retries, 3)} | "
            f"{_summary(candidate_retries, 3)} | "
            f"{_change(base_retries, candidate_retries)} |",
            "",
            "> Mixbench is scheduling-sensitive even after adaptive sampling; use it "
            "as a secondary concurrency signal, not a pass/fail verdict.",
            "",
            "## Focused one-key RMW contention",
            "",
            "Five writers repeatedly update one shared key. Values are medians with "
            "min–max ranges from three interleaved runs. Missing baseline protocol "
            "fields mean the baseline predates the focused summary schema; latency "
            "and completion counts remain comparable.",
            "",
            "| Metric | Main | PR | Change |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    deadlock_metrics = [
        ("Successful tx/run", "completed", 0),
        ("Completion tx/s", "tx_per_sec", 2),
        ("p50 latency ms", "p50_ms", 1),
        ("p90 latency ms", "p90_ms", 1),
        ("Retries/tx", "retries_per_tx", 3),
        ("Direct candidates/tx", "direct_candidates_per_tx", 3),
        ("Direct land rate", "direct_land_rate", 3),
        ("Worker drain ms", "worker_drain_ms", 1),
    ]
    for label, field, digits in deadlock_metrics:
        base = [getattr(run, field) for run in base_deadlock]
        candidate = [getattr(run, field) for run in candidate_deadlock]
        lines.append(
            f"| {label} | {_optional_summary(base, digits)} | "
            f"{_optional_summary(candidate, digits)} | "
            f"{_optional_change(base, candidate)} |"
        )
    lines.extend(
        [
            "",
            "> A missing cell, worker error, or incomplete drain fails the measurement "
            "step instead of producing a comparison.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--base-label", required=True)
    parser.add_argument("--candidate-label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    report = render_report(args.input, args.base_label, args.candidate_label)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
